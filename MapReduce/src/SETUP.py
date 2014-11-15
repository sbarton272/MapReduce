#!/usr/bin/python

from subprocess import call

# Compiles code
call(['javac', '-cp', '.', 'master/Master.java'])
call(['javac', '-cp', '.', 'participant/Participant.java'])

# Compile examples
call(['javac', '-cp', '.', 'examples/wordcount/MapWordCount.java'])
call(['javac', '-cp', '.', 'examples/wordcount/ReduceWordCount.java'])
call(['javac', '-cp', '.', 'examples/wordoccurences/MapWordOccurences.java'])
call(['javac', '-cp', '.', 'examples/wordoccurences/ReduceWordOccurences.java'])


# Compile test code
call(['javac', '-cp', '.', 'tests/testBasics.java'])
call(['javac', '-cp', '.', 'tests/testExamples.java'])
call(['javac', '-cp', '.', 'tests/testFileServer.java'])
call(['javac', '-cp', '.', 'tests/testRemoteFile.java'])

