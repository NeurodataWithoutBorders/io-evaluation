# The script reads different spans for use case 4, 5, 6 for oophys dataset and records the result as an hdf5 file
from pynwb import NWBHDF5IO
import pynwb
import math
import time
import sys
import random
import h5py
from time import perf_counter

def time_ms(t0,t1,x0,x1,y0,y1,nwbfile):
    start_eval = time.perf_counter_ns()
    nwbfile.acquisition["TwoPhotonSeries"].data[t0:t1, x0:x1, y0:y1]
    end_eval = time.perf_counter_ns()
    return (end_eval-start_eval)/math.pow(10,9) #time in seconds

def gen_random(n_timestamps, spans):
    num = random.randint(1, n_timestamps-spans)
    return num

def create_dataset(f, path_results, path_samples, path_span_txy, dim1, dim2, n_timestamps, span_len_t, dim_x, span_len_xy, dim_y, uc, nwbfile):
    f.require_dataset(path_span_txy + path_samples, shape = (dim1, dim2), dtype = "i")
    f.require_dataset(path_span_txy + path_results, shape = (dim1,), dtype = "f")
    for samples in range(0, dim1):
        t0 = gen_random(n_timestamps, span_len_t)
        if uc == 4:
            x0 = gen_random(dim_x, span_len_xy)
            y0 = gen_random(dim_y, span_len_xy)
            t1 = t0 + span_len_t
            x1 = x0 + span_len_xy
            y1 = y0 + span_len_xy
            f[path_span_txy + path_samples][samples,1] = x0
            f[path_span_txy + path_samples][samples,2] = y0
        else:
            x0 = 0
            x1 = dim_x
            y0 = 0
            y1 = dim_y
            if uc == 5:
                t1 = t0+ span_len_t
            else:
                t1 = t0 + 1
        exec_time=time_ms(t0, t1, x0, x1, y0, y1, nwbfile)
        #write results
        f[path_span_txy + path_results][samples] = exec_time
        f[path_span_txy + path_samples][samples,0]= t0

def main():
    # dim & n_timestamps obtained by inspecting oophys dataset
    n_timestamps = 40000
    dim_x = 796
    dim_y = 512

    configParam = int(sys.argv[1]) # provided by array job
    filepath = str(sys.argv[2]) # nwb file to be read
    outfile = str(sys.argv[3]) # path to output hdf5 file
    spans_t = [500, 1000, 2000, 4000] # spans across timestamps t
    spans_xy = [4, 8, 16, 32, 64] # spans across dim x,y
    max_samples = 100
    random.seed(30)

    with pynwb.NWBHDF5IO(filepath, "r", load_namespaces=True) as io:
        nwbfile = io.read()

        with h5py.File(outfile + str(configParam)+'.h5', 'a') as f:
            for uc in range(4,7): # 3 use cases tested
                path ='/usecase{usecase}/'.format(usecase = str(uc)) # path to use case
                for span_len_t in spans_t:
                  path_span_t = path + 't{span_t}/'.format(span_t= str(span_len_t)) # path to spans t (across timestamps), for every use case
                  if uc == 4:
                    for span_len_xy in spans_xy:
                      path_span_txy = path_span_t + 'xy{}/'.format(str(span_len_xy)) # path to spans across dim x,y for every span t, in each use case
                      path_results ='results' # stores results from reads
                      path_samples ='samples_t0x0y0' # stores sampled starting points across t, x, y
                      dim1 = max_samples
                      dim2 = 3
                      create_dataset(f, path_results, path_samples, path_span_txy, dim1, dim2, n_timestamps, span_len_t, dim_x, span_len_xy, dim_y, uc, nwbfile)
                  else:
                       path_span_txy = path
                       path_results = 'results'
                       path_samples = 'samples_t0' # stores sampled starting points across t
                       dim1 = max_samples
                       dim2 = 1
                       span_len_xy = 0
                       create_dataset(f, path_results, path_samples, path_span_txy, dim1, dim2, n_timestamps, span_len_t, dim_x, span_len_xy, dim_y, uc, nwbfile)

if __name__ == "__main__":
    main()
