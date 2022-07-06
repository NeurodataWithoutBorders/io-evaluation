#Created: by Urjoshi Sinha,06/14/2022
#Last modified: by Urjoshi Sinha, 06/30/2022

#Description: The script runs different compression, compression options & chunking options for two different slices of data for Dandiset53-sub-npI1_ses-20190413_behavior+ecephys.nwb
# and writes the output to an nwb file. Different spans of data are then read (using 2 different read patterns) from the new nwb file & the times noted.

from pynwb import NWBHDF5IO
import time
import os
from hdmf.data_utils import GenericDataChunkIterator
from hdmf.backends.hdf5.h5_utils import H5DataIO
import math
import pynwb
from os.path import exists

def time_ms(t2,t1):
    return str((t2-t1)/math.pow(10,6))

def main():
    # time required for the entire execution
    start_tot_time = time.time_ns()
    filepath = r"../sub-npI1_ses-20190413_behavior+ecephys.nwb" # Modify this according to the file path in your system

    # File Stats- timestamps:141969007   channels:384
    source_datalen = 140000000  # based on o/p from inspecting Dandiset53
    timestamp_slices = [1/10, 1/100]

    # Spans to read
    spans = [
        (0, 100000, 0, 384),
        (0, 100000, 100, 120),
        (0, 100000, 100, 101),
        (0, 500, 0, 384),
        (0, 500, 100, 120),
        (0, 500, 100, 101),
        ]

    header= 'ConfigNo Source_DataLen Span_Len(%) n_Timestamps T_Source_load_namespaces(ms) T_Source_Read(ms) T_Source_Iterator(ms) T_Source_CreateObj(ms) T_Target_Write(ms) Filesize T_P1_load_namespace(ms) T_P1_Read_io(ms) T_P1S1(ms) T_P1S2(ms) T_P1S3(ms) T_P1S4(ms) T_P1S5(ms) T_P1S6(ms) AutoChunkedVal T_P2S1(ms) T_P2S2(ms) T_P2S3(ms) T_P2S4(ms) T_P2S5(ms) T_P2S6(ms)\n'
    stats_f = open('trimmedData/stats_UC0.txt', 'a') # Modify this as needed
    stats_f.write(header)

    # Read from Configurations from file
    countConfig = 0
    with open('configurations.txt') as readConfigs_f:
        configset = readConfigs_f.readlines()
        for configs in configset:
            countConfig += 1
            if countConfig == 1: #Ignore file header
                continue
            else:
                # Set configurations, convert options to appropiate format
                params = configs.split()

                #Set chunksizes
                if params[0] != 'None' and params[0] != 'True':
                    chunkssizes = tuple(map(int, params[0].split(',')))
                elif params[0] == 'None':
                    chunkssizes = None
                elif params[0] == 'True':
                    chunkssizes = True

                #Set compression
                if params[1] != 'None':
                    compr = params[1]
                else:
                    compr = None

                #Set compression options
                if params[2] != 'None':
                    comp_op = int(params[2])
                else:
                    comp_op = None

                for slice_len in range(0, len(timestamp_slices)):
                    n_timestamps = int(timestamp_slices[slice_len]* source_datalen)
                    stats=str(countConfig - 1) + ' '+str(source_datalen) + ' '+ str(timestamp_slices[slice_len] * 100) + ' ' + str(n_timestamps) + ' '

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


                    # time required to load namespaces

                    start_time_load = time.time_ns()
                    with NWBHDF5IO(filepath, 'r', load_namespaces=True) as io:
                        end_time_load = time.time_ns()
                        net_time_load = time_ms(end_time_load, start_time_load) #Time in millisecs
                        stats=stats+str(net_time_load) + ' '

                        # time required for io.read()

                        start_time_ioread = time.time_ns()
                        nwbfile = io.read()
                        end_time_ioread = time.time_ns()
                        net_time_ioread = time_ms(end_time_ioread, start_time_ioread)
                        stats=stats+str(net_time_ioread) + ' '

                        orig_eseries = nwbfile.acquisition['ElectricalSeries']
                        electrodes = nwbfile.create_electrode_table_region(region=orig_eseries.electrodes.data[:].tolist(),
                                name=orig_eseries.electrodes.name,
                                description=orig_eseries.electrodes.description)

                        num_electrodes = orig_eseries.data.shape[1]
                        max_timestamps = int(n_timestamps)

                        # the original dataset is already chunked. for optimal read, read 1 chunk at a time by
                        # setting the read chunk shape to align with the dataset chunk shape

                        assert orig_eseries.data.chunks
                        selection_size_time = orig_eseries.data.chunks[0]

                        # read the electricalseries data iteratively in chunks because it is too big to fit into RAM

                        start_time_iterator = time.time_ns()
                        data_iterator = H5DatasetDataChunkIterator(dataset=nwbfile.acquisition['ElectricalSeries'].data, max_length=max_timestamps,
                                chunk_shape=(selection_size_time,
                                num_electrodes), buffer_gb=8)
                        end_time_iterator = time.time_ns()
                        net_time_iterator = time_ms(end_time_iterator, start_time_iterator)
                        stats=stats+str(net_time_iterator) + ' '


                        # time required for creating object

                        start_time_createObj = time.time_ns()
                        # create an H5DataIO object which sets HDF5-specific filters and other write options
                        data = H5DataIO(data=data_iterator,
                                        compression=compr,
                                        compression_opts=comp_op,
                                        chunks=chunkssizes)

                        end_time_createObj = time.time_ns()
                        net_time_createObj = time_ms(end_time_createObj, start_time_createObj)
                        stats=stats+str(net_time_createObj) + ' '

                        # create the new electricalseries with the same parameters as the original electricalseries

                        new_eseries = pynwb.ecephys.ElectricalSeries(
                            name=orig_eseries.name,
                            description=orig_eseries.description,
                            data=data,
                            electrodes=electrodes,
                            starting_time=orig_eseries.starting_time,
                            rate=orig_eseries.rate,
                            conversion=orig_eseries.conversion,
                            resolution=orig_eseries.resolution,
                            comments=orig_eseries.comments,
                            )

                        nwbfile.acquisition.pop('ElectricalSeries')  # remove the existing electricalseries
                        nwbfile.add_acquisition(new_eseries)  # add the newly chunked electricalseries

                        nwbfile.processing.pop('ecephys')  # remove the ecephys processing module
                        trimmed_dataset ='trimmedData/dandiset53_Slice'+ str(slice_len + 1) + 'Config' + str(countConfig- 1) + '.nwb' # Modify this as needed

                        # time required for write

                        start_time_write = time.time_ns()
                        with pynwb.NWBHDF5IO(trimmed_dataset, 'w',manager=io.manager) as export_io:
                            export_io.export(io, nwbfile)
                        end_time_write = time.time_ns()
                        net_time_write = time_ms(end_time_write, start_time_write)
                        stats=stats+str(net_time_write) + ' '

                    # if file exists, get file size
                    f_exists=exists(trimmed_dataset)
                    if f_exists==1:
                        filesize=os.path.getsize(trimmed_dataset)
                        stats=stats+str(filesize)+' '
                    else:
                        stats=stats+'Error '

                    # Read from generated file
                    # Pattern 1:

                    target_start_time_load = time.time_ns()
                    with NWBHDF5IO(trimmed_dataset, 'r',load_namespaces=True) as io:
                            target_end_time_load = time.time_ns()
                            target_net_time_load = time_ms(target_end_time_load, target_start_time_load)
                            stats=stats+str(target_net_time_load) + ' '

                            # time required for io.read()
                            target_start_time_readio = time.time_ns()
                            nwbfile = io.read()
                            target_end_time_readio = time.time_ns()
                            target_net_time_readio = time_ms(target_end_time_readio, target_start_time_readio)
                            stats=stats+str(target_net_time_readio) + ' '

                            for span in spans:
                            # time required for reading spans
                                target_start_time_readSpan = time.time_ns()
                                data = nwbfile.acquisition['ElectricalSeries'].data[span[0]:span[1], span[2]:span[3]]
                                target_end_time_readSpan = time.time_ns()
                                target_net_time_readSpan =time_ms(target_end_time_readSpan, target_start_time_readSpan)
                                stats=stats+str(target_net_time_readSpan)+ ' '

                            # Determine chunk size for chunking option= True (auto-chunking)
                            if params[0] == 'True':
                                c_size = nwbfile.acquisition['ElectricalSeries'].data.chunks
                                #stats=stats+str(c_size)+' '
                                dim1,dim2=c_size
                                stats=stats+str(dim1)+','+str(dim2)+' '
                            else:
                                stats=stats+'N/A '

                        # Pattern 2:

                    for span in spans:
                        #time required for 'load_namespaces' and 'io.read()'' need not be checked here, as it has been performed earlier
                            with NWBHDF5IO(trimmed_dataset, 'r',load_namespaces=True) as io:
                                nwbfile = io.read()

                                #  time required for reading spans
                                target_start_time_readSpan = time.time_ns()
                                data = nwbfile.acquisition['ElectricalSeries'].data[span[0]:span[1], span[2]:span[3]]
                                target_end_time_readSpan = time.time_ns()
                                target_net_time_readSpan = time_ms(target_end_time_readSpan, target_start_time_readSpan)
                                stats=stats+str(target_net_time_readSpan)+ ' '
                    stats_f.write(stats+'\n')


    end_tot_time = time.time_ns()
    net_tot_time = time_ms(end_tot_time, start_tot_time)
    stats_f.write('totalTime:: ' + str(net_tot_time) + '\n')
    stats_f.close()

if __name__ == "__main__":
    main()
