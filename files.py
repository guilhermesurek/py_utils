import glob
import os
from zipfile import ZipFile
import concurrent.futures

some_dir_path = 'test_dir'

def list_files_in_dir(dir_path, ext=None):
    if ext == None:
        files_list = glob.glob("%s/*" % dir_path)
    else:
        files_list = glob.glob("%s/*%s" % (dir_path, ext))
    return files_list

def list_files_in_zip(zip_path, ext=None):
    zip_file = ZipFile(zip_path, 'r')
    if ext == None:
        files_list = zip_file.namelist()
    else:
        files_list = [name for name in zip_file.namelist() if name.endswith(ext)]
    return files_list

def _count_file(fn):
    with open(fn, 'rb') as f:
        return _count_file_object(f)

def _count_file_object(f):
    # Reference: https://www.peterbe.com/plog/fastest-way-to-unzip-a-zip-file-in-python
    # Note that this iterates on 'f'.
    # You *could* do 'return len(f.read())'
    # which would be faster but potentially memory 
    # inefficient and unrealistic in terms of this 
    # benchmark experiment. 
    total = 0
    for line in f:
        total += len(line)
    return total

def unzip_member_f3(zip_filepath, filename, dest):
    with open(zip_filepath, 'rb') as f:
        zf = ZipFile(f)
        zf.extract(filename, dest)
    fn = os.path.join(dest, filename)
    return _count_file(fn)

def f3(fn, dest):
    #with open(fn, 'rb') as f:
        #zf = ZipFile(f)
        zf = ZipFile(fn)
        futures = []
        with concurrent.futures.ProcessPoolExecutor() as executor:
            for member in zf.infolist():
                futures.append(
                    executor.submit(
                        unzip_member_f3,
                        fn,
                        member.filename,
                        dest,
                    )
                )
            total = 0
            for future in concurrent.futures.as_completed(futures):
                total += future.result()
        return total

if __name__ == "__main__":
    print('#1 - List files in directory')
    print(list_files_in_dir(some_dir_path))
    print('#2 - List files in directory filtering by extension')
    print(list_files_in_dir(some_dir_path, ".txt"))
    print('#3 - List files inside compressed files')
    print(list_files_in_zip(some_dir_path + '/zip_files.zip'))
    print('#3 - List files inside compressed files filtering by extension')
    print(list_files_in_zip(some_dir_path + '/zip_files.zip', ".mp4"))