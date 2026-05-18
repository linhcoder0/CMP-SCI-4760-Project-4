#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/wait.h>
#include <sys/msg.h>
#include <signal.h>
#include <errno.h>
#include <stdarg.h>
#include <time.h>

#define MAX_PCB_SIZE 18
#define NANOPERSEC 1000000000LL
#define BASE_QUANTUM_NS 25000000
#define BLOCK_TIME_NS 100000000
#define PRINT_INTERVAL_NS 500000000LL
#define MAX_LOG_LINES 10000

const size_t BUFF_SZ = sizeof(unsigned int) * 2;


struct PCB {
    int occupied;
    int localPid;
    pid_t pid;

    int startSeconds;
    int startNano;

    int serviceTimeSeconds;
    int serviceTimeNano;

    int eventWaitSec;
    int eventWaitNano;

    int blocked;
    int messagesSent;
};

struct Message {
    long mtype;
    int value;  // OSS -> worker: quantum. Worker -> OSS: used time, negative if terminating.
    int pid;
    int slot;
};

struct Options {
    int n;
    int s;
    double t;
    double i;
    char logFile[256];
};

struct PCB pcbTable[MAX_PCB_SIZE];

static volatile sig_atomic_t shutdownFlag = 0;
static volatile sig_atomic_t shutdownSig = 0;

static int shm_id_global = -1;
static int msg_id_global = -1;
static unsigned int *clock_global = NULL;

static int readyQueue[MAX_PCB_SIZE];
static int readyFront = 0;
static int readyRear = 0;
static int readyCount = 0;

static int logLineCount = 0;
static int logLimitMessagePrinted = 0;

void signal_handler(int sig) {
    shutdownFlag = 1;
    shutdownSig = sig;
}

void logBoth(FILE *logFile, const char *format, ...) {
    va_list args;

    va_start(args, format);
    vprintf(format, args);
    va_end(args);

    fflush(stdout);

    if (logFile != NULL) {
        if (logLineCount < MAX_LOG_LINES) {
            va_start(args, format);
            vfprintf(logFile, format, args);
            va_end(args);

            fflush(logFile);
            logLineCount++;
        } else if (!logLimitMessagePrinted) {
            fprintf(logFile, "OSS: Log line limit reached. Further file logging stopped.\n");
            fflush(logFile);
            logLimitMessagePrinted = 1;
        }
    }
}

long long getClockNS(unsigned int *clock) {
    return ((long long)clock[0] * NANOPERSEC) + clock[1];
}

void setClockFromNS(unsigned int *clock, long long totalNS) {
    if (totalNS < 0) {
        totalNS = 0;
    }

    clock[0] = (unsigned int)(totalNS / NANOPERSEC);
    clock[1] = (unsigned int)(totalNS % NANOPERSEC);
}

void addToClock(unsigned int *clock, long long amountNS) {
    long long totalNS = getClockNS(clock);
    totalNS += amountNS;
    setClockFromNS(clock, totalNS);
}

long long secondsToNS(double seconds) {
    if (seconds <= 0) {
        return 0;
    }

    return (long long)(seconds * NANOPERSEC);
}

long long randBetweenLL(long long min, long long max) {
    if (max <= min) {
        return min;
    }

    double r = (double)rand() / ((double)RAND_MAX + 1.0);
    return min + (long long)(r * (double)(max - min + 1));
}

void printUsage(const char *programName) {
    printf("Usage: %s [-h] [-n proc] [-s simul] [-t timelimitForChildren] "
           "[-i fractionOfSecondToLaunchChildren] [-f logfile]\n", programName);
}

void parseOptions(int argc, char *argv[], struct Options *options) {
    options->n = 5;
    options->s = 2;
    options->t = 1.0;
    options->i = 0.1;
    strcpy(options->logFile, "log.txt");

    int opt;
    while ((opt = getopt(argc, argv, "hn:s:t:i:f:")) != -1) {
        switch (opt) {
            case 'h':
                printUsage(argv[0]);
                exit(EXIT_SUCCESS);

            case 'n':
                options->n = atoi(optarg);
                break;

            case 's':
                options->s = atoi(optarg);
                break;

            case 't':
                options->t = atof(optarg);
                break;

            case 'i':
                options->i = atof(optarg);
                break;

            case 'f':
                strncpy(options->logFile, optarg, sizeof(options->logFile) - 1);
                options->logFile[sizeof(options->logFile) - 1] = '\0';
                break;

            default:
                printUsage(argv[0]);
                exit(EXIT_FAILURE);
        }
    }

    if (options->n <= 0) {
        options->n = 1;
    }

    if (options->s <= 0) {
        options->s = 1;
    }

    if (options->s > MAX_PCB_SIZE) {
        options->s = MAX_PCB_SIZE;
    }

    if (options->t <= 0) {
        options->t = 1.0;
    }

    if (options->i < 0) {
        options->i = 0.0;
    }
}

void clearPCBEntry(int slot) {
    pcbTable[slot].occupied = 0;
    pcbTable[slot].localPid = 0;
    pcbTable[slot].pid = 0;

    pcbTable[slot].startSeconds = 0;
    pcbTable[slot].startNano = 0;

    pcbTable[slot].serviceTimeSeconds = 0;
    pcbTable[slot].serviceTimeNano = 0;

    pcbTable[slot].eventWaitSec = 0;
    pcbTable[slot].eventWaitNano = 0;

    pcbTable[slot].blocked = 0;
    pcbTable[slot].messagesSent = 0;
}

void initPCBTable(void) {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        clearPCBEntry(i);
    }
}

int findFreePCBSlot(void) {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 0) {
            return i;
        }
    }

    return -1;
}

void addServiceTime(int slot, int usedNS) {
    long long currentService =
        ((long long)pcbTable[slot].serviceTimeSeconds * NANOPERSEC) +
        pcbTable[slot].serviceTimeNano;

    currentService += usedNS;

    pcbTable[slot].serviceTimeSeconds = (int)(currentService / NANOPERSEC);
    pcbTable[slot].serviceTimeNano = (int)(currentService % NANOPERSEC);
}

int isSlotInReadyQueue(int slot) {
    for (int i = 0; i < readyCount; i++) {
        int index = (readyFront + i) % MAX_PCB_SIZE;
        if (readyQueue[index] == slot) {
            return 1;
        }
    }

    return 0;
}

int enqueueReady(int slot) {
    if (readyCount >= MAX_PCB_SIZE) {
        return -1;
    }

    if (isSlotInReadyQueue(slot)) {
        return 0;
    }

    readyQueue[readyRear] = slot;
    readyRear = (readyRear + 1) % MAX_PCB_SIZE;
    readyCount++;

    return 0;
}

int dequeueReady(void) {
    if (readyCount <= 0) {
        return -1;
    }

    int slot = readyQueue[readyFront];
    readyFront = (readyFront + 1) % MAX_PCB_SIZE;
    readyCount--;

    return slot;
}

void printReadyQueue(FILE *logFile) {
    logBoth(logFile, "OSS: Ready queue [ ");

    for (int i = 0; i < readyCount; i++) {
        int index = (readyFront + i) % MAX_PCB_SIZE;
        int slot = readyQueue[index];

        if (pcbTable[slot].occupied) {
            logBoth(logFile, "P%d ", pcbTable[slot].localPid);
        }
    }

    logBoth(logFile, "]\n");
}

void printBlockedProcesses(FILE *logFile) {
    logBoth(logFile, "Blocked processes: [ ");

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].blocked) {
            logBoth(logFile, "P%d until %d:%d ",
                    pcbTable[i].localPid,
                    pcbTable[i].eventWaitSec,
                    pcbTable[i].eventWaitNano);
        }
    }

    logBoth(logFile, "]\n");
}

void printProcessTable(FILE *logFile, unsigned int *clock) {
    logBoth(logFile, "\nOSS PID:%d SysClockS: %u SysclockNano: %u\n",
            getpid(), clock[0], clock[1]);

    logBoth(logFile, "Process Table:\n");
    logBoth(logFile,
            "Entry Occ LocalPID RealPID StartS StartN ServiceS ServiceN Blocked EventS EventN MsgSent\n");

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        logBoth(logFile,
                "%5d %3d %8d %7d %6d %6d %8d %8d %7d %6d %6d %7d\n",
                i,
                pcbTable[i].occupied,
                pcbTable[i].localPid,
                (int)pcbTable[i].pid,
                pcbTable[i].startSeconds,
                pcbTable[i].startNano,
                pcbTable[i].serviceTimeSeconds,
                pcbTable[i].serviceTimeNano,
                pcbTable[i].blocked,
                pcbTable[i].eventWaitSec,
                pcbTable[i].eventWaitNano,
                pcbTable[i].messagesSent);
    }

    printBlockedProcesses(logFile);
    logBoth(logFile, "\n");
}

void cleanupIPC(void) {
    if (clock_global != NULL && clock_global != (void *)-1) {
        shmdt(clock_global);
        clock_global = NULL;
    }

    if (shm_id_global != -1) {
        shmctl(shm_id_global, IPC_RMID, NULL);
        shm_id_global = -1;
    }

    if (msg_id_global != -1) {
        msgctl(msg_id_global, IPC_RMID, NULL);
        msg_id_global = -1;
    }
}

void killRunningChildren(void) {
    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied == 1 && pcbTable[i].pid > 0) {
            kill(pcbTable[i].pid, SIGTERM);
        }
    }
}

long long getEventWaitNS(int slot) {
    return ((long long)pcbTable[slot].eventWaitSec * NANOPERSEC) +
           pcbTable[slot].eventWaitNano;
}

long long findNextBlockedWakeupNS(void) {
    long long nextWakeup = -1;

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].blocked) {
            long long eventNS = getEventWaitNS(i);

            if (nextWakeup == -1 || eventNS < nextWakeup) {
                nextWakeup = eventNS;
            }
        }
    }

    return nextWakeup;
}

void checkBlockedProcesses(FILE *logFile, unsigned int *clock, long long *totalOverheadNS) {
    long long currentNS = getClockNS(clock);

    for (int i = 0; i < MAX_PCB_SIZE; i++) {
        if (pcbTable[i].occupied && pcbTable[i].blocked) {
            long long eventNS = getEventWaitNS(i);

            if (currentNS >= eventNS) {
                pcbTable[i].blocked = 0;

                enqueueReady(i);

                long long unblockOverhead = randBetweenLL(1000, 5000);
                addToClock(clock, unblockOverhead);
                *totalOverheadNS += unblockOverhead;

                logBoth(logFile,
                        "OSS: Unblocking process P%d PID %d at time %u:%u, overhead %lld ns\n",
                        pcbTable[i].localPid,
                        (int)pcbTable[i].pid,
                        clock[0],
                        clock[1],
                        unblockOverhead);

                currentNS = getClockNS(clock);
            }
        }
    }
}

int main(int argc, char *argv[]) {
    struct Options options;
    parseOptions(argc, argv, &options);

    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    signal(SIGALRM, signal_handler);
    alarm(3);

    srand((unsigned int)(time(NULL) ^ getpid()));

    FILE *logFilePtr = fopen(options.logFile, "w");
    if (logFilePtr == NULL) {
        perror("OSS: Error opening log file");
        return EXIT_FAILURE;
    }

    initPCBTable();

    logBoth(logFilePtr, "OSS: starting, PID:%d PPID:%d\n", getpid(), getppid());
    logBoth(logFilePtr, "OSS called with:\n");
    logBoth(logFilePtr, "-n %d\n", options.n);
    logBoth(logFilePtr, "-s %d\n", options.s);
    logBoth(logFilePtr, "-t %.3f\n", options.t);
    logBoth(logFilePtr, "-i %.3f\n", options.i);
    logBoth(logFilePtr, "-f %s\n", options.logFile);

    key_t shm_key = ftok("oss.c", 0);
    if (shm_key == (key_t)-1) {
        perror("OSS: ftok shared memory");
        fclose(logFilePtr);
        return EXIT_FAILURE;
    }

    int shm_id = shmget(shm_key, BUFF_SZ, IPC_CREAT | 0700);
    if (shm_id == -1) {
        perror("OSS: shmget");
        fclose(logFilePtr);
        return EXIT_FAILURE;
    }

    unsigned int *clock = (unsigned int *)shmat(shm_id, NULL, 0);
    if (clock == (void *)-1) {
        perror("OSS: shmat");
        shmctl(shm_id, IPC_RMID, NULL);
        fclose(logFilePtr);
        return EXIT_FAILURE;
    }

    shm_id_global = shm_id;
    clock_global = clock;

    clock[0] = 0;
    clock[1] = 0;

    key_t msg_key = ftok("oss.c", 1);
    if (msg_key == (key_t)-1) {
        perror("OSS: ftok message queue");
        cleanupIPC();
        fclose(logFilePtr);
        return EXIT_FAILURE;
    }

    int msg_id = msgget(msg_key, IPC_CREAT | 0700);
    if (msg_id == -1) {
        perror("OSS: msgget");
        cleanupIPC();
        fclose(logFilePtr);
        return EXIT_FAILURE;
    }

    msg_id_global = msg_id;

    logBoth(logFilePtr, "OSS: initialized shared memory clock and message queue id %d\n", msg_id);

    int launchedChildren = 0;
    int activeChildren = 0;
    int finishedChildren = 0;
    int totalMessagesSent = 0;

    long long totalCpuTimeNS = 0;
    long long totalOverheadNS = 0;
    long long totalIdleNS = 0;

    long long launchIntervalNS = secondsToNS(options.i);
    long long nextLaunchNS = 0;
    long long nextPrintNS = PRINT_INTERVAL_NS;

    long long maxBurstNS = secondsToNS(options.t);
    if (maxBurstNS <= 0) {
        maxBurstNS = NANOPERSEC;
    }

    while (!shutdownFlag && (launchedChildren < options.n || activeChildren > 0)) {
        long long currentNS = getClockNS(clock);

        while (!shutdownFlag &&
               launchedChildren < options.n &&
               activeChildren < options.s &&
               currentNS >= nextLaunchNS) {

            int slot = findFreePCBSlot();
            if (slot == -1) {
                logBoth(logFilePtr, "OSS: No available PCB slot for new child\n");
                break;
            }

            long long burstNS = randBetweenLL(1, maxBurstNS);
            int burstSec = (int)(burstNS / NANOPERSEC);
            int burstNano = (int)(burstNS % NANOPERSEC);

            pcbTable[slot].occupied = 1;
            pcbTable[slot].localPid = launchedChildren + 1;
            pcbTable[slot].pid = 0;
            pcbTable[slot].startSeconds = clock[0];
            pcbTable[slot].startNano = clock[1];
            pcbTable[slot].serviceTimeSeconds = 0;
            pcbTable[slot].serviceTimeNano = 0;
            pcbTable[slot].eventWaitSec = 0;
            pcbTable[slot].eventWaitNano = 0;
            pcbTable[slot].blocked = 0;
            pcbTable[slot].messagesSent = 0;

            pid_t pid = fork();

            if (pid == -1) {
                perror("OSS: fork");
                clearPCBEntry(slot);
                shutdownFlag = 1;
                break;
            }

            if (pid == 0) {
                char secStr[32];
                char nanoStr[32];

                snprintf(secStr, sizeof(secStr), "%d", burstSec);
                snprintf(nanoStr, sizeof(nanoStr), "%d", burstNano);

                execl("./worker", "worker", secStr, nanoStr, (char *)NULL);

                perror("OSS: execl failed");
                exit(EXIT_FAILURE);
            }

            pcbTable[slot].pid = pid;
            enqueueReady(slot);

            launchedChildren++;
            activeChildren++;

            logBoth(logFilePtr,
                    "OSS: Generating process P%d with PID %d and putting it in ready queue at time %u:%u. Total CPU burst: %d:%d\n",
                    pcbTable[slot].localPid,
                    (int)pid,
                    clock[0],
                    clock[1],
                    burstSec,
                    burstNano);

            long long launchOverhead = randBetweenLL(1000, 10000);
            addToClock(clock, launchOverhead);
            totalOverheadNS += launchOverhead;

            currentNS = getClockNS(clock);
            nextLaunchNS = currentNS + launchIntervalNS;

            if (launchIntervalNS > 0) {
                break;
            }
        }

        checkBlockedProcesses(logFilePtr, clock, &totalOverheadNS);

        currentNS = getClockNS(clock);
        if (currentNS >= nextPrintNS) {
            printProcessTable(logFilePtr, clock);

            while (nextPrintNS <= currentNS) {
                nextPrintNS += PRINT_INTERVAL_NS;
            }
        }

        if (readyCount > 0) {
            printReadyQueue(logFilePtr);

            int slot = dequeueReady();

            if (slot < 0 || slot >= MAX_PCB_SIZE || !pcbTable[slot].occupied) {
                continue;
            }

            long long dispatchOverhead = randBetweenLL(100, 1000);
            addToClock(clock, dispatchOverhead);
            totalOverheadNS += dispatchOverhead;

            logBoth(logFilePtr,
                    "OSS: Dispatching process P%d PID %d from ready queue at time %u:%u\n",
                    pcbTable[slot].localPid,
                    (int)pcbTable[slot].pid,
                    clock[0],
                    clock[1]);

            logBoth(logFilePtr,
                    "OSS: total time this dispatch was %lld nanoseconds\n",
                    dispatchOverhead);

            struct Message msg;
            msg.mtype = pcbTable[slot].pid;
            msg.value = BASE_QUANTUM_NS;
            msg.pid = getpid();
            msg.slot = slot;

            if (msgsnd(msg_id, &msg, sizeof(struct Message) - sizeof(long), 0) == -1) {
                if (errno != EINTR) {
                    perror("OSS: msgsnd");
                }
                shutdownFlag = 1;
                break;
            }

            pcbTable[slot].messagesSent++;
            totalMessagesSent++;

            struct Message reply;
            if (msgrcv(msg_id, &reply, sizeof(struct Message) - sizeof(long), 1, 0) == -1) {
                if (errno != EINTR) {
                    perror("OSS: msgrcv");
                }
                shutdownFlag = 1;
                break;
            }

            int usedNS = abs(reply.value);

            addToClock(clock, usedNS);
            totalCpuTimeNS += usedNS;
            addServiceTime(slot, usedNS);

            logBoth(logFilePtr,
                    "OSS: Receiving that process P%d PID %d ran for %d nanoseconds\n",
                    pcbTable[slot].localPid,
                    (int)pcbTable[slot].pid,
                    usedNS);

            if (reply.value < 0) {
                logBoth(logFilePtr,
                        "OSS: Process P%d PID %d terminated after using %d nanoseconds of its time quantum\n",
                        pcbTable[slot].localPid,
                        (int)pcbTable[slot].pid,
                        usedNS);

                int status = 0;
                waitpid(pcbTable[slot].pid, &status, 0);

                clearPCBEntry(slot);
                activeChildren--;
                finishedChildren++;
            } else if (usedNS < BASE_QUANTUM_NS) {
                logBoth(logFilePtr,
                        "OSS: Process P%d PID %d did not use its entire time quantum\n",
                        pcbTable[slot].localPid,
                        (int)pcbTable[slot].pid);

                pcbTable[slot].blocked = 1;

                long long wakeupNS = getClockNS(clock) + BLOCK_TIME_NS;
                pcbTable[slot].eventWaitSec = (int)(wakeupNS / NANOPERSEC);
                pcbTable[slot].eventWaitNano = (int)(wakeupNS % NANOPERSEC);

                logBoth(logFilePtr,
                        "OSS: Putting process P%d PID %d into blocked queue until %d:%d\n",
                        pcbTable[slot].localPid,
                        (int)pcbTable[slot].pid,
                        pcbTable[slot].eventWaitSec,
                        pcbTable[slot].eventWaitNano);
            } else {
                logBoth(logFilePtr,
                        "OSS: Process P%d PID %d used its full quantum. Putting it back into ready queue\n",
                        pcbTable[slot].localPid,
                        (int)pcbTable[slot].pid);

                enqueueReady(slot);
            }
        } else {
            long long nowNS = getClockNS(clock);
            long long nextEventNS = -1;

            if (launchedChildren < options.n && activeChildren < options.s) {
                nextEventNS = nextLaunchNS;
            }

            long long nextWakeupNS = findNextBlockedWakeupNS();

            if (nextWakeupNS != -1 &&
                (nextEventNS == -1 || nextWakeupNS < nextEventNS)) {
                nextEventNS = nextWakeupNS;
            }

            long long idleNS;

            if (nextEventNS != -1 && nextEventNS > nowNS) {
                idleNS = nextEventNS - nowNS;
            } else {
                idleNS = 1000;
            }

            logBoth(logFilePtr,
                    "OSS: No ready processes at time %u:%u. Advancing clock by %lld nanoseconds\n",
                    clock[0],
                    clock[1],
                    idleNS);

            addToClock(clock, idleNS);
            totalIdleNS += idleNS;
        }
    }

    if (shutdownFlag) {
        logBoth(logFilePtr,
                "OSS: Received shutdown signal %d, shutting down and cleaning up\n",
                shutdownSig);

        killRunningChildren();

        while (wait(NULL) > 0) {
            // Reap all children.
        }
    }

    long long totalElapsedNS = getClockNS(clock);
    double cpuUtilization = 0.0;

    if (totalElapsedNS > 0) {
        cpuUtilization = ((double)totalCpuTimeNS / (double)totalElapsedNS) * 100.0;
    }

    logBoth(logFilePtr, "\nOSS: Final Report\n");
    logBoth(logFilePtr, "OSS: Launched workers: %d\n", launchedChildren);
    logBoth(logFilePtr, "OSS: Finished workers: %d\n", finishedChildren);
    logBoth(logFilePtr, "OSS: Total messages sent: %d\n", totalMessagesSent);
    logBoth(logFilePtr, "OSS: Final simulated time: %u:%u\n", clock[0], clock[1]);
    logBoth(logFilePtr, "OSS: Total CPU time used by workers: %lld ns\n", totalCpuTimeNS);
    logBoth(logFilePtr, "OSS: Total OSS overhead time: %lld ns\n", totalOverheadNS);
    logBoth(logFilePtr, "OSS: Total idle time: %lld ns\n", totalIdleNS);
    logBoth(logFilePtr, "OSS: CPU utilization: %.2f%%\n", cpuUtilization);

    alarm(0);
    cleanupIPC();
    fclose(logFilePtr);

    return EXIT_SUCCESS;
}