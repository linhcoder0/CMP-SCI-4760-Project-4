#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>
#include <time.h>
#include <errno.h>

#define NANOPERSEC 1000000000LL

const size_t BUFF_SZ = sizeof(unsigned int) * 2;

struct Message {
    long mtype;
    int value;  // OSS -> worker: quantum. Worker -> OSS: used time, negative if terminating.
    int pid;
    int slot;
};

int randBetweenInt(int min, int max) {
    if (max <= min) {
        return min;
    }

    return min + rand() % (max - min + 1);
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        fprintf(stderr, "WORKER: Usage: %s burstSeconds burstNanoseconds\n", argv[0]);
        return EXIT_FAILURE;
    }

    int burstSec = atoi(argv[1]);
    int burstNano = atoi(argv[2]);

    if (burstSec < 0) {
        burstSec = 0;
    }

    if (burstNano < 0) {
        burstNano = 0;
    }

    while (burstNano >= NANOPERSEC) {
        burstSec++;
        burstNano -= NANOPERSEC;
    }

    long long totalCpuNeededNS =
        ((long long)burstSec * NANOPERSEC) + burstNano;

    if (totalCpuNeededNS <= 0) {
        totalCpuNeededNS = 1;
    }

    srand((unsigned int)(time(NULL) ^ getpid()));

    key_t shm_key = ftok("oss.c", 0);
    if (shm_key == (key_t)-1) {
        perror("WORKER: ftok shared memory");
        return EXIT_FAILURE;
    }

    int shm_id = shmget(shm_key, BUFF_SZ, 0700);
    if (shm_id == -1) {
        perror("WORKER: shmget");
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);
    if (clock == (void *)-1) {
        perror("WORKER: shmat");
        return EXIT_FAILURE;
    }

    key_t msg_key = ftok("oss.c", 1);
    if (msg_key == (key_t)-1) {
        perror("WORKER: ftok message queue");
        shmdt(clock);
        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, 0700);
    if (msg_id == -1) {
        perror("WORKER: msgget");
        shmdt(clock);
        return EXIT_FAILURE;
    }

    long long totalCpuUsedNS = 0;
    int done = 0;

    while (!done) {
        struct Message msg;

        if (msgrcv(msg_id,
                   &msg,
                   sizeof(struct Message) - sizeof(long),
                   getpid(),
                   0) == -1) {
            if (errno != EINTR) {
                perror("WORKER: msgrcv");
            }

            shmdt(clock);
            return EXIT_FAILURE;
        }

        int quantum = msg.value;
        if (quantum <= 0) {
            quantum = 1;
        }

        long long remainingNS = totalCpuNeededNS - totalCpuUsedNS;

        int usedNS = 0;
        int responseValue = 0;

        if (remainingNS <= quantum) {
            usedNS = (int)remainingNS;

            if (usedNS <= 0) {
                usedNS = 1;
            }

            totalCpuUsedNS += usedNS;
            responseValue = -usedNS;
            done = 1;
        } else {
            int shouldBlock = (rand() % 100) < 20;

            if (shouldBlock) {
                if (quantum > 1) {
                    usedNS = randBetweenInt(1, quantum - 1);
                } else {
                    usedNS = 1;
                }

                if (usedNS > remainingNS) {
                    usedNS = (int)remainingNS;
                }

                totalCpuUsedNS += usedNS;

                if (totalCpuUsedNS >= totalCpuNeededNS) {
                    responseValue = -usedNS;
                    done = 1;
                } else {
                    responseValue = usedNS;
                }
            } else {
                usedNS = quantum;
                totalCpuUsedNS += usedNS;
                responseValue = usedNS;
            }
        }

        struct Message reply;
        reply.mtype = 1;
        reply.value = responseValue;
        reply.pid = getpid();
        reply.slot = msg.slot;

        if (msgsnd(msg_id,
                   &reply,
                   sizeof(struct Message) - sizeof(long),
                   0) == -1) {
            perror("WORKER: msgsnd");
            shmdt(clock);
            return EXIT_FAILURE;
        }
    }

    shmdt(clock);
    return EXIT_SUCCESS;
}