from __init__ import Logger
import pandas as pd
import numpy as np
import csv
import os
from datetime import datetime


def Main(filePath, childFile, destfolder, size, RowChunkSize, hdrfileName = 'hdr', lineitemFileName = 'line'):
    hdrfileName = os.path.basename(filePath)
    hdrfileName = os.path.splitext(hdrfileName)[0]
    
    #read main file
    print('Reading Header/Main file data: Starting')
    dataset = ReadAndSortData(filePath, RowChunkSize)
    print('Reading Header/Main file data: Completed')

    start = 0
    end = len(dataset)
    noOffiles = int(end/size) + 1
    print('Writting files - Started')
    for i in range(0, noOffiles, 1):
        total = start+size
        data_extracted = dataset[start:total-1]
        fileName = '{}{}{}{}{}.txt'.format(destfolder,"\\", hdrfileName, "_", i+1)
        np.savetxt(fileName, data_extracted, delimiter ='\t', newline='', fmt='%s')
        print('file ' + fileName + ' created successfully.')
        #look for child files.
        if len(childFile) > 0:
            linefiles = childFile.split(",")
            for file in linefiles:
                CreateSubFile(file, destfolder, i, data_extracted[0], RowChunkSize)
        start = total
    print('Writting files - Completed')

def CreateSubFile(filePath, destfolder, loop, dataRange, RowChunkSize):
    fileName = os.path.basename(filePath)
    fileName = os.path.splitext(fileName)[0]
    chunk_list = []
    #filtering subfile data based on first row of main file
    chunk_dataset = pd.read_csv(filePath, sep='\t', lineterminator='\n', header = None, dtype=str, encoding='ISO-8859-1', low_memory=False, chunksize = RowChunkSize)
    for chunk in chunk_dataset:
        #replacing NAN string with empty which was for emtpy values.
        chunk.replace(np.nan, '', regex=True, inplace = True)
        #look all child rows (i.e foreign key data) in each child data set chunk
        extracted_chunk = (chunk[(chunk[0].values >= dataRange.values[0]) & (chunk[0].values <= dataRange.values[len(dataRange)-1])])
        chunk_list.append(extracted_chunk)
    # concat the list into dataframe 
    dataset = pd.concat(chunk_list)
    fileName = '{}{}{}{}{}.txt'.format(destfolder,"\\", fileName, "_", loop+1)
    #write the filter chunk data to a file
    np.savetxt(fileName, dataset, delimiter ='\t', newline='', fmt='%s')
    print('file ' + fileName + ' created successfully.')
    


def ReadAndSortData(filePath, RowChunkSize):
    chunk_list = []
    #read data
    chunk_dataset = pd.read_csv(filePath, sep='\t', lineterminator='\n', header = None, dtype=str, encoding='ISO-8859-1', low_memory=False, chunksize = RowChunkSize)
    for chunk in chunk_dataset:
        #replacing nan with empty which was for emtpy values.
        chunk.replace(np.nan, '', regex=True, inplace = True)  
        chunk_list.append(chunk)
    # concat the list into dataframe 
    dataset = pd.concat(chunk_list)
    #sort based on column 0 values
    dataset.sort_values(0, ascending = True, inplace = True)
    return dataset


if __name__ == '__main__':
    try:
        #row chunk size to read at a time to get benefit of memory
        RowChunkSize = 50000
        parentFile = str(input('Path of the Main(Parent) file:'))
        fileSize = int(input('Enter the no. of rows size to write in splitted files:'))
        isSubFiles = str(input('Do you have child/related-sub files (y/n):'))
        childFile = ''
        if((isSubFiles == 'Y') | (isSubFiles == 'y')):
            childFile = str(input('Path (comma seperated) of child/related-sub file:'))
        destfolder = str(input('Destination folder to drop files after splitting:'))
        
        print('Starting the splitting process at %s.Please wait...' % datetime.now())
        Main(parentFile, childFile, destfolder, fileSize, RowChunkSize)
        print('Splitting process successfully completed at %s. Please check destination folder.' % datetime.now())
    except Exception as e:
        print('Splitting process failed with an error: '+ repr(e))
        Logger.error(e)