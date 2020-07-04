#include "mio.h"
#include <fstream>
#include <vector>
#include <omp.h>
#include <windows.h>
#include <string>
#include <iostream>

using namespace std;

// 1 GB
constexpr size_t numBytes = 1024 *1024 * 1024;
size_t numCount = numBytes / sizeof(double);
double* numbers = new double[numCount];

static double WriteEachByte()
{
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
      file.write(reinterpret_cast<const char*>(&numbers[i]), sizeof(numbers[0]));
  }
  return omp_get_wtime() - start;
}

static double WriteAllBytes()
{
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
  }
  return omp_get_wtime() - start;
}

int handle_error(const std::error_code& error)
{
  const auto& errmsg = error.message();
  printf("error mapping file: %s, exiting...\n", errmsg.c_str());
  return error.value();
}

HANDLE CreateSparseFile(LPCTSTR lpSparseFileName)
{
  // Use CreateFile as you would normally - Create file with whatever flags
  //and File Share attributes that works for you
  DWORD dwTemp;

  HANDLE hSparseFile = CreateFile(lpSparseFileName,
      GENERIC_READ | GENERIC_WRITE,
      FILE_SHARE_READ | FILE_SHARE_WRITE,
      NULL,
      CREATE_ALWAYS,
      FILE_ATTRIBUTE_NORMAL,
      NULL);

  if (hSparseFile == INVALID_HANDLE_VALUE)
    return hSparseFile;

  DeviceIoControl(hSparseFile,
      FSCTL_SET_SPARSE,
      NULL,
      0,
      NULL,
      0,
      &dwTemp,
      NULL);
  return hSparseFile;
}

DWORD SetSparseRange(HANDLE hSparseFile, LONGLONG start, LONGLONG size)
{
  // Specify the starting and the ending address (not the size) of the
  // sparse zero block
  FILE_ZERO_DATA_INFORMATION fzdi;
  fzdi.FileOffset.QuadPart = start;
  fzdi.BeyondFinalZero.QuadPart = start + size;
  // Mark the range as sparse zero block
  DWORD dwTemp;
  SetLastError(0);
  BOOL bStatus = DeviceIoControl(hSparseFile,
      FSCTL_SET_ZERO_DATA,
      &fzdi,
      sizeof(fzdi),
      NULL,
      0,
      &dwTemp,
      NULL);
  if (bStatus)
    return 0; //Sucess
  else
  {
    DWORD e = GetLastError();
    return (e); //return the error value
  }
}

void createSparseFileWindows(string fname)
{
  try
  {
    HANDLE h = CreateSparseFile(fname.c_str());
    if (h == INVALID_HANDLE_VALUE)
    {
      cerr << "Unable to create file" << endl;
      return;
    }
    if (SetSparseRange(h, 0, numBytes) != 0)
    {
      cerr << "Unable to set sparse range" << endl;
      return;
    }
    LARGE_INTEGER seek;
    seek.QuadPart = numBytes;
    if (!SetFilePointerEx(h, seek, 0, 0))
    {
      cerr << "Unable to seek to desired offset" << endl;
      return;
    }
    SetEndOfFile(h);
    CloseHandle(h);
  }
  catch (const exception& ex)
  {
    cerr << ex.what() << endl;
  }
}

static double MmapEachByte()
{
  double start = omp_get_wtime();
  {
    // create file
    string filePath = "test3.bin";
    createSparseFileWindows(filePath);
    /*
    FILE* fp = fopen(filePath.c_str(), "w");
    fseek(fp, numBytes, SEEK_SET);
    fputc('\n', fp);
    fclose(fp);*/

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
  // RANDOM allocated memory
  printf("WriteEachByte: %f\n", WriteEachByte());
  // WriteEachByte<double> : 25.231502
  printf("WriteAllBytes: %f\n", WriteAllBytes());
  // WriteAllBytes<double> : 4.390023
  printf("MmapEachByte: %f\n", MmapEachByte());
  // MmapEachByte<double>: 3.294356


  system("pause");
  return 0;
}