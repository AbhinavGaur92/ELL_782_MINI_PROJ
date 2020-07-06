# ELL_782_MINI_PROJ

The Mini Project entails designing and implementing a parallel algorithm of your choice (please discuss this with the instructor before starting off) on a programming platform of your choice - C/C++/Java on Linux/Windows. 
You could work on Sorting and Searching Algorithms, DFT/FFT etc. You could design your own algorithms, or implement existing algorithms.

# Installation

pip install mpi4py

# Usage

python Main_M_BS.py NUM_PROC ELE   # Runs Binary Search Algorithm on 2D Mesh architecture ; Number of parallel processes = NUM_PROC ; Search Element = ELE

python Main_M_MS.py NUM_PROC   # Runs Merge Sort Algorithm on a predefined array on 2D Mesh architecture ; Number of parallel processes = NUM_PROC

python Main_LR_BS.py NUM_PROC ELE   # Runs Binary Search Algorithm on Linear Ring architecture ; Number of parallel processes = NUM_PROC ; Search Element = ELE

python Main_LR_MS.py NUM_PROC   # Runs Merge Sort Algorithm on a perdefined array on Linear Ring architecture ; Number of parallel processes = NUM_PROC ; 
