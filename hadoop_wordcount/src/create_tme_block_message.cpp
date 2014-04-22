#include <iostream>
#include <fstream>
#include <stdint.h>
using namespace std;

std::ifstream::pos_type filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::in | std::ifstream::binary);
    in.seekg(0, std::ifstream::end);
    return in.tellg();
}

int main(int argc, char **args)
{
// open file

uint32_t file_size = filesize(args[1]);

std::cout << "file size = " << file_size << std::endl;


    ifstream myFile (args[1], ios::in | ios::binary);
    char* memblock = new char [file_size];

    myFile.read (memblock, file_size);
    myFile.close();

    cout << "the complete file content is in memory";

    uint32_t big_file_size = __builtin_bswap32(file_size);
    std::cout << "big endien=" << big_file_size << std::endl;

    string output_file(args[1]);
    output_file += ".2";
    ofstream binary_file(output_file.c_str(), ios::out|ios::binary|ios::app);
    binary_file.write((char*)&big_file_size, 4);
          binary_file.write(memblock, file_size);
          binary_file.close();
    delete[] memblock;

// get file size
// create 4 byte big-endian, value is file size
// save to another file

return 0;
}
