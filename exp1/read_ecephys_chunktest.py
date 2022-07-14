# Created: by Urjoshi Sinha, 07/02/2022
# Last modified: by Urjoshi Sinha, 07/12/2022

# The script reads different spans for use cases 1, 2, 3 for ecephys dataset and records the result as an hdf5 file

from pynwb import NWBHDF5IO
import math
import pynwb
import sys
import random
import time
import h5py
from time import perf_counter

def time_ms(t0,t1,x0,x1, nwbfile):
    start_eval = time.perf_counter_ns()
    data = nwbfile.acquisition['ElectricalSeries'].data[t0:t1,x0:x1]
    end_eval = time.perf_counter_ns()
    return (end_eval-start_eval)/math.pow(10,9) #time in seconds

def main():
    configParam = int(sys.argv[1]) # provided by array job
    filepath = str(sys.argv[2]) # path to input nwb file
    outfile = str(sys.argv[3]) # path to output hdf5 file
    n_timestamps = 340000000  # n_timestamps & channels obtained by inspecting Dandiset 53
    channels = 384
    spans = [500, 1000, 2000, 4000, 8000, 2**16, 2**20, 2**24] # spans across timestamps t
    random.seed(30)
    path_samples_t ='samples_t0' # stores sampled starting points across timestamps t
    path_samples_x ='samples_x0' # stores sampled starting points across channels
    path_results ='time_read' # stores results from reads
    inp_nwb = 'dandiset53_ecephys_Config{config}.nwb'.format(config = str(configParam)) # nwb file name
    out_h5 = 'readtests_ecephys_config{config}.h5'.format(config = str(configParam)) # output hdf5 file
    span_xy_uc2 = 20

    with pynwb.NWBHDF5IO(filepath + inp_nwb, 'r', load_namespaces= True) as io:
        nwbfile = io.read()
        for uc in range(1,4):
            for span_len in spans:
                rand_list=[]
                if span_len <= 8000:
                    max_samples = 1000
                else:
                    max_samples = 10

                for samples in range(max_samples):
                    rand_list.append(random.randint(0, n_timestamps - span_len))

                with h5py.File(outfile + out_h5, 'a') as f:
                    path='/usecase{usecase}/span{sl}'.format(usecase = str(uc), sl = str(span_len)) # path to use case
                    f.require_dataset(path + path_samples_t, shape = (max_samples,), dtype = "i")
                    f.require_dataset(path + path_results, shape = (max_samples,), dtype = "f")

                    for sample_no, t0 in enumerate(rand_list):
                        t1 = t0 + span_len
                        # read spans
                        if uc == 1:
                            x0 = 0
                            x1 = channels
                        elif uc == 2:
                            f.require_dataset(path+path_samples_x, shape = (max_samples,), dtype = "i")
                            x0 = random.randint(0, channels-span_xy_uc2)
                            x1 = x0 + span_xy_uc2
                            f[path][path_samples_x][sample_no] = x0
                        elif uc == 3:
                            f.require_dataset(path+path_samples_x, shape = (max_samples,), dtype = "i")
                            x0 = random.randint(0, channels)
                            x1 = x0 + 1
                            f[path][path_samples_x][sample_no] = x0
                        exec_time = time_ms(t0, t1, x0, x1, nwbfile)

                        # write results
                        f[path][path_samples_t][sample_no] = t0
                        f[path][path_results][sample_no] = exec_time

if __name__ == "__main__":
    main()
