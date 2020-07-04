#include "mio.h"
#include <fstream>
#include <vector>
#include <omp.h>

using namespace std;

// 1 GB
constexpr size_t numBytes = 1024 * 1024 * 1024;

template <typename T>
static double WriteEachByte()
{
  size_t numCount = numBytes / sizeof(T);
  T* numbers = new T[numCount];

  double start = omp_get_wtime();
  {
    // create if doesn't exist
    string filePath = "test1.bin";
    ofstream file;
    file.open(filePath, ios::out | ios::app);
    file.close();
    // Open the file in the mode we want
    file.open(filePath, ios::in | ios::out | ios::binary | ios::ate);
    // Write each 4 byte value at a time
    for (size_t i = 0; i < numCount; i++)
      file.write(reinterpret_cast<const char*>(&numbers[i]), sizeof(T));
  }
  return omp_get_wtime() - start;
}

template <typename T>
static double WriteAllBytes()
{
  size_t numCount = numBytes / sizeof(T);
  T* numbers = new T[numCount];

  double start = omp_get_wtime();
  {
    // create if doesn't exist
    string filePath = "test2.bin";
    ofstream file;
    file.open(filePath, ios::out | ios::app);
    file.close();
    // Open the file in the mode we want
    file.open(filePath, ios::in | ios::out | ios::binary | ios::ate);
    // Write ALL bytes at same time
    file.write(reinterpret_cast<const char*>(numbers), numBytes);
    file.flush();
    file.close();
  }
  return omp_get_wtime() - start;
}

int handle_error(const std::error_code& error)
{
  const auto& errmsg = error.message();
  printf("error mapping file: %s, exiting...\n", errmsg.c_str());
  return error.value();
}

template <typename T>
static double MmapEachByte()
{
  int numCount = numBytes / sizeof(T);
  T* numbers = new T[numCount];

  double start = omp_get_wtime();
  {
    // create if doesn't exist
    string filePath = "test3.bin";
    ofstream file;
    file.open(filePath, ios::out | ios::app);
    file.close();
    // Open the file in the mode we want
    file.open(filePath, ios::in | ios::out | ios::binary | ios::ate);
    // Allocate space for file...
    file.write(reinterpret_cast<const char*>(numbers), numBytes);
    file.flush();
    file.close();

    // Now mmap
    error_code error;
    mio::mmap_sink rw_mmap = mio::make_mmap_sink(filePath, 0, mio::map_entire_file, error);
    if (error)
      return handle_error(error);

    // As fast as writing to memory!
    memcpy(rw_mmap.data_, numbers, numBytes);
    // As we go out of scope both sync() and munmap() are called
  }
  return omp_get_wtime() - start;
}

int main()
{
  printf("WriteEachByte<double>: %f\n", WriteEachByte<double>());
  // WriteEachByte<double>: 22.781654
  printf("WriteAllBytes<double>: %f\n", WriteAllBytes<double>());
  // WriteAllBytes<double>: 4.478148
  printf("MmapEachByte<double>: %f\n", MmapEachByte<double>());
  // MmapEachByte<double>: 7.093659
  system("pause");
  return 0;
}