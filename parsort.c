#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Merge the elements in the sorted ranges [begin, mid) and [mid, end),
// copying the result into temparr.

int compare_i64(const void *a, const void *b){
  const int64_t *x = a, *y = b;
  if(*x < *y){
    return -1;
  }
  else if (*x > *y){
    return 1;
  }
  else{
    return 0;
  }
}

//h
void merge(int64_t *arr, size_t begin, size_t mid, size_t end, int64_t *temparr) {
  int64_t *endl = arr + mid;
  int64_t *endr = arr + end;
  int64_t *left = arr + begin, *right = arr + mid, *dst = temparr;

  for (;;) {
    int at_end_l = left >= endl;
    int at_end_r = right >= endr;

    if (at_end_l && at_end_r) break;

    if (at_end_l)
      *dst++ = *right++;
    else if (at_end_r)
      *dst++ = *left++;
    else {
      int cmp = compare_i64(left, right);
      if (cmp <= 0)
        *dst++ = *left++;
      else
        *dst++ = *right++;
    }
  }
}

// void merge_sort(int64_t *arr, size_t begin, size_t end, size_t threshold) {
//   // TODO: implement

   
//   if ((end - begin) <= threshold) {
//     qsort(arr+begin, end - begin, sizeof(int64_t), compare_i64);

//   } else {
//     merge_sort(arr, begin, begin + (end-begin)/2, threshold);
//     merge_sort(arr, begin + (end-begin)/2, end, threshold);
    
//     int64_t temp[end-begin];

//     merge(arr, begin, (end+begin)/2, end, temp);
    
//     for(size_t i = 0; i < end-begin; i++){
//       *(arr+begin+i) = temp[i];
//     }

//   }
// }
void merge_sort(int64_t *arr, size_t begin, size_t end, size_t threshold) {

  if ((end - begin) <= threshold) {
    qsort(arr+begin, end - begin, sizeof(int64_t), compare_i64);
  } else {

    pid_t pid = fork();
    if (pid == -1) {
      exit(-1);
    } else if (pid == 0) {
    // this is now in the child process
      merge_sort(arr, begin, begin + (end-begin)/2, threshold);
      exit(0);

    }

    

    pid_t pid_2 = fork();
    if (pid_2 == -1) {
      exit(-1);
    } else if (pid_2 == 0) {
    // this is now in the child process
      merge_sort(arr, begin + (end-begin)/2, end, threshold);
      exit(0);

    }

      int wstatus, wstatus_2;
      // blocks until the process indentified by pid_to_wait_for completes
      pid_t actual_pid = waitpid(pid, &wstatus, 0);
      if (actual_pid == -1) {
        exit(0);
      }
      
      pid_t actual_pid_2 = waitpid(pid_2, &wstatus_2, 0);
      if (actual_pid_2 == -1) {
        exit(0);
      }
      
    int64_t temp[end-begin];
    merge(arr, begin, (end+begin)/2, end, temp);

    for(size_t i = 0; i < end-begin; i++){
      *(arr+begin+i) = temp[i];
    }

  } 
}


int main(int argc, char **argv) {
  
  // check for correct number of command line arguments
  if (argc != 3) {
    fprintf(stderr, "Usage: %s <filename> <sequential threshold>\n", argv[0]);
    return 1;
  }

  // process command line arguments
  const char *filename = argv[1];
  char *end;
  size_t threshold = (size_t) strtoul(argv[2], &end, 10);
  if (end != argv[2] + strlen(argv[2])) {
    fprintf(stderr, "Invalid threshold value!\n"); }; 

  /*int64_t arr[] = {98, 47, 100, 123, 3, 342, 22, 22};
  merge_sort(arr, 0, 8, 3);
  printf("final output\n");
  for(int i = 0; i < 8; i++){
    printf("%ld\n", arr[i]);
  }*/

  // open the file
  int fd = open(filename, O_RDWR);
  if (fd < 0) {
    fprintf(stderr, "File couldn't be opened!\n");
    exit(1);
  }
  
  // use fstat to determine the size of the file
  struct stat statbuf;
  int rc = fstat(fd, &statbuf);
  if (rc != 0) {
    fprintf(stderr, "Unsuccessful fstat call!\n");
    exit(1);
  }
  size_t file_size_in_bytes = statbuf.st_size;

  // map the file into memory using mmap
  int64_t *data = mmap(NULL, file_size_in_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);

  if (data == MAP_FAILED) {
    fprintf(stderr, "Unsuccessful mmaping!\n");
    exit(1);
  }

  // sort the data!
  size_t end_arg = file_size_in_bytes / sizeof(int64_t);
  merge_sort(data, 0, end_arg, threshold);

  // unmap and close the file
  int munmap_val = munmap(data, file_size_in_bytes);
  if (munmap_val == -1) {
    fprintf(stderr, "Unsuccessful unmapping!\n");
    exit(1);
  }

  int close_val = close(fd);
  if (close_val < 0) {
    fprintf(stderr, "File couldn't be closed!\n");
    exit(1);
  }
  
  // exit with a 0 exit code if sort was successful
  return 0;
}