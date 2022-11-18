#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>

/*
*   1) GLOBAL DEFINE AND STRUCTS:
*       - TRUE: define as "1" for easier references .
*       - struct chunkResult: stores the result of each chunk encoded.
*       - struct task: stores the variables necessary for each worker 
*                      thread to encode.
*/
#define TRUE 1
typedef struct chunkResult{
    // actual result string
    char* result;
    // size of result for fwrite in main thread
    int sizeOfResult;
    // last char and count of it preserved for stitching in main thread
    char lastCharAndCount[2];
} chunkResult;
typedef struct task{
    // function pointer for each worker thread to complete task
    chunkResult (*function)(char*, int, int);
    // mmap file pointer so each task knows what to encode
    char* filePointer;
    // index the encoding should start at in filePointer
    int startIndex;
    // index the encoding should stop at in filePointer
    int endIndex;
    // stores result of it's encoding
    chunkResult result;
} task;

/*
*   2) (GLOBAL VARIABLES) - CONSTRUCTING/ASSIGNING TASKS:
*       - task taskQ[]: stores tasks that needs to be fetched by worker threads.
*                       tasks constructed by main thread and inserted here.
*       - numOfTasks: functions same way as a queue. Used as condition variable
*                     for worker threads to wait until there is a task.
*       - fetchIndex: used in worker threads to fetch task to execute. This is 
*                     so that the tasks are fetched in order constructed.
*       - addToQIndex: index used to add tasks to queue by the main thread.
*       - queueMutex: mutex used to lock any thread that reads/writes to any of 
*                     the above global variables. Used for synchronization to 
*                     avoid race conditions. 
*       - taskAvailableCondition: condition variable that worker threads will 
*                                 wait on. This will be signified by main thread.
*/
task taskQ[250010];
int numOfTasks = 0;
int fetchIndex = 0;
int addToQIndex = 0;
pthread_mutex_t queueMutex;
pthread_cond_t taskAvailableCondition;

/*
*   3) (GLOBAL VARIABLES) - SETTING/RECEIVING RESULTS:
*       - pthread_cond_t consumeArray[]: array that stores condition variables which
*                                        which will be waited on by main thread. Worker
*                                        threads will signal the index mirrored to the
*                                        task index mapped in their taskQ. 
*       - collectMutex: mutex used to lock any worker thread when setting their results, 
*                       and locking main thread when checking if result has been set.
*/
pthread_cond_t consumeArray[250010];
pthread_mutex_t collectMutex;

/*
*   4) MAIN THREAD FUNCTIONS: 
*       - main(): initializes global variables and parses command line for job option. 
*                 Also calls to construct tasks for worker threads as well as stitching
*                 and printing worker results when they are finished.
*       - constructTasks(): constructs tasks based on files in argv[]. Calls addTaskToQ
*                           to then add constructed tasks to taskQ.
*       - addTaskToQ(): add singular task to taskQ[] at the next available index. Also 
*                       adds to numOfTasks and sends a signal to worker threads that 
*                       a new task is available.
*       - stitchAndPrintRes(): Main thread waits on condition variable mapped to index
*                              of task in taskQ which is signaled by the worker thread
*                              that fetches that task once it is done encoding and has 
*                              written back the encoded result to the task. 
*/
int main(int argc, char *argv[]) {
    // if no argument is passed 
    if(argc == 1){
        return 0;
    }
    // get option
    int opt = 0;
    // default number of worker threads = 1
    int threadCount = 1;
    // store start index of files to be opened
    int startIndex = 1;
    // if there is an option argument
    while((opt = getopt(argc, argv, "j:")) != -1){
        switch(opt){
            case 'j':
                // overwrite default threadCount
                threadCount = atoi(optarg);
                // change start index of file reading
                startIndex = optind;
        }
    }
    // construct threadPool based on threadcount 
    pthread_t threadPool[threadCount];
    // initialize mutex and condition variable for task queue modifying
    pthread_mutex_init(&queueMutex, NULL);
    pthread_cond_init(&taskAvailableCondition, NULL);
    // initialize mutex for collecting results
    pthread_mutex_init(&collectMutex, NULL);
    // declare function that each thread will run
    void* runThread();
    // create worker threads
    for(int i = 0; i < threadCount; i++){
        pthread_create(&threadPool[i], NULL, &runThread, NULL);
    }
    // construct tasks 
    void constructTasks();
    constructTasks(argv, startIndex, argc);
    // stitch and print task results
    void stitchAndPrintRes();
    stitchAndPrintRes(addToQIndex);
    return 0;
}
void constructTasks(char* argv[], int startIndex, int endIndex){
    // declare runTask function to assign it to function pointer in task struct
    chunkResult runTask(char* chunk, int encodeStartIndex, int encodeEndIndex);
    // file pointer
    char* filePointer;
    // file descriptor
    int fd; 
    // struct sb (for size of file) 
    struct stat sb; 
    // loop through each file in cmd line 
    for (int i = startIndex; i < endIndex; i++){
        // open file for reading
        fd = open(argv[i], O_RDONLY, S_IRUSR | S_IWUSR);
        // if file aint open-able
        if (fd == -1){
            perror("nyuenc");
            return;
        }
        // if sb aint fstat-able
        if (fstat(fd, &sb) == -1){
            perror("nyuenc");
            return;
        }
        // map file to file pointer
        filePointer = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
        // if file size is smaller than 4KB
        if (sb.st_size <= 4096){
            // create new task
            task newTask;
            // assign function pointer to worker thread function
            newTask.function = &runTask;
            // assign filePointer to currently mapped file
            newTask.filePointer = filePointer;
            // assign start index to beginning of file
            newTask.startIndex = 0;
            // assign end index to end of file
            newTask.endIndex = sb.st_size;
            // result is initialized as NULL (not done)
            newTask.result.result = NULL;
            // add task to Q
            void addTaskToQ();
            addTaskToQ(newTask);
        }
        // if file size is larger than 4KB
        else{
            // number of task per file
            int numOfTaskPerFile = 1;
            // if the file size is divisible by 4096
            if(sb.st_size%4096 == 0){
                // number of tasks will be exact equal chunks of 4096
                numOfTaskPerFile = sb.st_size/4096;
            }
            // if the file size is not divisible by 4096
            else{
                // number of tasks per file will be not exact equal chunks of 4096
                numOfTaskPerFile += sb.st_size/4096;
            }
            // track startIndex for assigning tasks
            int currentStartIndex = 0;
            // loop through each denom of 4KB in file
            for (int j = 0; j < numOfTaskPerFile; j++){
                // create new task
                task newTask;
                // if last task in file
                if (j == numOfTaskPerFile-1){
                    // assign function pointer to worker thread function
                    newTask.function = &runTask;
                    // assign filePointer to current mapped file
                    newTask.filePointer = filePointer;
                    // assign start index to current tracked start index
                    newTask.startIndex = currentStartIndex;
                    // assign end index to end of file index
                    newTask.endIndex = sb.st_size;
                    // initialize result as NULL (not done)
                    newTask.result.result = NULL;
                    // add task to q
                    void addTaskToQ();
                    addTaskToQ(newTask);
                }
                // first task or middle task
                else{
                    // assign function pointer to worker thread function
                    newTask.function = &runTask;
                    // assign filePointer to current mapped file
                    newTask.filePointer = filePointer;
                    // assign start index to current tracked start index
                    newTask.startIndex = currentStartIndex;
                    // assign end index as current tracked start index + 4096 (4kb chunk)
                    currentStartIndex += 4096;
                    newTask.endIndex = currentStartIndex;
                    // initialize result as NULL (not done)
                    newTask.result.result = NULL;
                    // add task to q
                    void addTaskToQ();
                    addTaskToQ(newTask);
                }
            }
        }
    }
    return;
}
void addTaskToQ(task newTask){
    // lock global variables used for queue since worker threads will need to read from it
    pthread_mutex_lock(&queueMutex);
    // add a new task to number of tasks
    numOfTasks+=1;
    // add 1 to addToQIndex so that main thread will know next available spot in global taskQ
    addToQIndex+=1;
    // add new task to taskQ at the current available index
    taskQ[addToQIndex-1] = newTask;
    // construct a new condition variable for each task
    pthread_cond_init(&consumeArray[addToQIndex-1], NULL);
    // signal worker threads to keep working
    pthread_cond_signal(&taskAvailableCondition);
    // unlock mutex 
    pthread_mutex_unlock(&queueMutex);
    return;
}
void stitchAndPrintRes(int tasksInQ){
    // loop through every result task in queue, IN ORDER
    for (int i = 0; i < tasksInQ; i++){
        // main thread waits for signal from worker threads that this result is done
        pthread_mutex_lock(&collectMutex);
        while(!taskQ[i].result.result){
            pthread_cond_wait(&consumeArray[i], &collectMutex);
        }
        pthread_mutex_unlock(&collectMutex);
        // if first task result
        if (i == 0){
            chunkResult firstChunkResult = taskQ[0].result;
            // write result to stdout (NOT WRITING LAST CHAR AND COUNT PRESERVED)
            fwrite(firstChunkResult.result, sizeof(char), firstChunkResult.sizeOfResult, stdout);
            // free result malloced in thread function
            free(taskQ[0].result.result);
        }
        // if not first task result
        else{
            chunkResult currResult = taskQ[i].result;
            chunkResult prevResult = taskQ[i-1].result;
            // if previous chunk last processed character is the same as the current chunk first char
            if (prevResult.lastCharAndCount[0] == currResult.result[0]){
                // grab the count of the previous chunk char
                __uint8_t newCount = prevResult.lastCharAndCount[1];
                // add current first chunk char count
                newCount+= currResult.result[1];
                // re-assign count of current chunk result 
                currResult.result[1] = newCount;
                // write result to stdout (NOT WRITING LAST CHAR AND COUNT PRESERVED)
                fwrite(currResult.result, sizeof(char), currResult.sizeOfResult, stdout);
                // free result malloced in thread function
                free(taskQ[i].result.result);
            }
            // if both unique characters
            else{
                // write previous chunk's preserved result's last character and count to stdout
                fwrite(prevResult.lastCharAndCount, sizeof(char), 2, stdout);
                // write current chunk's result to stdout
                fwrite(currResult.result, sizeof(char), currResult.sizeOfResult, stdout);
                // free result malloced in thread function
                free(taskQ[i].result.result);
            }
        }
    }
    // write the last char and count in result preserved
    fwrite(taskQ[tasksInQ-1].result.lastCharAndCount, sizeof(char), 2, stdout);
    return;
}

/*
*   5) WORKER THREAD FUNCTIONS: 
*       - runThread(): the "main" function of the worker threads will first wait on
*                      the condition variable signaled by the main thread that says
*                      there is an available task. It will then fetch the next task
*                      not taken by other worker threads and process it. Once the
*                      threads are finished, it's result will be set back to the
*                      queue.
*       - fetchTask(): fetches the next available task.
*       - runTask(): all worker threads will execute this function simultaneously. 
*                    Each thread will process the chunk it is given and return it's
*                    processing results as chunkResult struct.
*/
void* runThread(){
    while (TRUE){
        // declare a new task
        task newTask;
        // lock queueMutex because worker thread is fetching from global queue variables
        pthread_mutex_lock(&queueMutex);
        // thread waits if task queue is empty
        while(numOfTasks == 0){
            pthread_cond_wait(&taskAvailableCondition, &queueMutex);
        }
        // fetch task from next available index
        task fetchTask();
        // store index task was fetched at in unique worker thread stack
        int queueIndex = fetchIndex;
        newTask = fetchTask(queueIndex);
        pthread_mutex_unlock(&queueMutex);
        
        // run newTask fetched
        task* taskPointer = &newTask;
        // produce result mapped in task pointer
        taskPointer->result = taskPointer->function(taskPointer->filePointer, 
        taskPointer->startIndex, taskPointer->endIndex);
        
        // lock control of collecting results
        pthread_mutex_lock(&collectMutex);
        // insert result back to task
        taskQ[queueIndex].result = taskPointer->result;
        // signal main thread that result at queueIndex has been finished.
        pthread_cond_signal(&consumeArray[queueIndex]);
        pthread_mutex_unlock(&collectMutex);
    }
}
task fetchTask(int index){
    // fetch task from queue
    task newTask = taskQ[index];
    // increase fetch index
    fetchIndex++;
    // remove task from taskQ
    numOfTasks-=1;
    return newTask;
}
chunkResult runTask(char* chunk, int encodeStartIndex, int encodeEndIndex){
    // store result - POTENTIALLY double the original size of chunk and null
    char* encResult = (char*) malloc(sizeof(char)*8195);
    // store last character
    char lastChar = '\0';
    // initialize and store occurences
    __uint8_t count = 1;
    // current Result index
    int curResIndex = 0;
    // iterate through each character in chunk
    for (int i = encodeStartIndex; i < encodeEndIndex; i++){
        // if first character of chunk
        if (i == encodeStartIndex){
            // set lastChar as first character
            lastChar = chunk[i];
        }
        // if not first character of chunk
        else {
            // if same character is found
            if (chunk[i] == lastChar){
                // increase count
                count++;
            }
            // if new character is found
            else{
                // set char
                encResult[curResIndex] = lastChar;
                curResIndex++;
                // set count
                encResult[curResIndex] = count;
                curResIndex++;
                // set lastChar as first character
                lastChar = chunk[i];
                // reset count
                count = 1;
            }
        } 
    }
    // construct result
    chunkResult encodeResult;
    encodeResult.sizeOfResult = curResIndex;
    encodeResult.result = encResult;
    // set char
    encodeResult.lastCharAndCount[0] = lastChar;
    encodeResult.lastCharAndCount[1] = count;
    // result of chunk now in encResult array
    return encodeResult;
}


/*
*   WORKS CITED (EXTERNAL SOURCES THAT INFLUENCED MY PROJECT):
*       
*       https://man7.org/linux/man-pages/man2/mmap.2.html
*       https://www.youtube.com/watch?v=m7E9piHcfr4&t=134s
*       https://man7.org/linux/man-pages/man3/getopt.3.html
*       https://www.geeksforgeeks.org/getopt-function-in-c-to-parse-command-line-arguments/
*       https://www.youtube.com/watch?v=0sVGnxg6Z3k&t=275s
*       https://code-vault.net/lesson/w1h356t5vg:1610029047572
*
*/
