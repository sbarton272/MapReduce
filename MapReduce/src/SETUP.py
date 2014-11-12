#!/usr/bin/python

from subprocess import call

# Compiles code
call(['javac', '-cp', '.', 'master/Master.java'])
call(['javac', '-cp', '.', 'participant/Participant.java'])

# Compile test code
call(['javac', '-cp', '.', 'tests/testBasics.java'])
call(['javac', '-cp', '.', 'tests/testExamples.java'])
call(['javac', '-cp', '.', 'tests/FileServer.java'])
call(['javac', '-cp', '.', 'tests/testRemoteFile.java'])

