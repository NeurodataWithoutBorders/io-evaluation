# The script checks for blocks of zeros in the oophys dataset

from pynwb import NWBHDF5IO
from hdmf.data_utils import GenericDataChunkIterator
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb
import sys

filepath= sys.argv[1] # File tested: sub-R6_ses-20200206T210000_behavior+ophys.nwb

with NWBHDF5IO(filepath, 'r', load_namespaces=True) as io:
    nwbfile = io.read()
    orig_eseries = nwbfile.acquisition['TwoPhotonSeries']
    data = orig_eseries.data
    for x in range(0, data.shape[0]): # Check for every timestamp: if data contains large blocks of zero
        array = data[x,:,:]
        if ((array == 0).all()):
            print(x)
