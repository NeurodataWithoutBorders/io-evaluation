# The script plots the data across the first four channels for all timestamps, to check for large blocks of zeros

import h5py
import pynwb
import matplotlib.pyplot as plt
import numpy as np

plot_colors = ['b','g','y','r']
filepath = sys.argv[1] # File tested: sub-npI3_ses-20190421_behavior+ecephys.nwb

with pynwb.NWBHDF5IO(filepath, 'r', load_namespaces= True) as io:
    nwbfile = io.read()
    orig_eseries = nwbfile.acquisition['ElectricalSeries']
    data = orig_eseries.data[:,0:4]
    size_dim1 = data.shape[0]
    plt.title(" Samples from electrophysiology data")
    plt.xlabel('Point in Time')
    plt.ylabel("Voltage")
    xpoints = range(0, size_dim1)
    xp = xpoints[::3] # data has been downsampled for plotting
    for channel in range(0,data.shape[1]):
            ypoints = np.array(data[:,channel])
            yp = ypoints[::3]
            plt.plot(xp, yp, plot_colors[channel], label= channel)
            plt.legend()
plt.savefig('plot_ecephys_channels.pdf', bbox_inches= 'tight')
plt.show()
