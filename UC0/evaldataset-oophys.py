from pynwb import NWBHDF5IO
from hdmf.data_utils import GenericDataChunkIterator
from hdmf.backends.hdf5.h5_utils import H5DataIO
import pynwb

filepath= 'sub-R6_ses-20200206T210000_behavior+ophys.nwb' #Modify filepath as needed
class H5DatasetDataChunkIterator(GenericDataChunkIterator):

    """A data chunk iterator that reads chunks over the 0th dimension of an HDF5 dataset up to a max length.
    """

    def __init__(
        self,
        dataset,
        max_length,
        **kwargs
        ):
        self.dataset = dataset
        self.max_length = max_length
        super().__init__(**kwargs)

    def _get_data(self, selection):
        return self.dataset[selection]

    def _get_maxshape(self):
        return (self.max_length, self.dataset.shape[1])

    def _get_dtype(self):
        return self.dataset.dtype

with NWBHDF5IO(filepath, 'r', load_namespaces=True) as io:
    nwbfile = io.read()
    orig_eseries = nwbfile.acquisition['TwoPhotonSeries']
    data = orig_eseries.data
    for x in range(0,data.shape[0]): #Check for every timestamp: whether data contains large time blocks of zeros
        array=data[x,:,:]
        if ((array==0).all()):
            print(x)
