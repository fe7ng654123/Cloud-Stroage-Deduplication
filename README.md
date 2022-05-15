# Cloud-Stroage-Deduplication

CSCI4180 (Fall 2020)
Assignment 3: Deduplication


1 Introduction
This assignment is to implement a simple storage application using deduplication. The deduplication is
based on Rabin fingerprinting.
2 System Overview

You’re going to implement a single Java program called MyDedup. MyDedup supports the following oper-
ations: upload, download, and delete.

2.1 Upload
The upload operation includes the following functions:

• Chunking. It reads in the pathname of a file and divides the input file into chunks using Rabin finger-
printing.

• Identifying unique chunks. Only unique data chunks will be uploaded to the cloud. We use a finger-
print index to record and identify unique chunks.
