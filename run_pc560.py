from konsensus import KonsensusApp

app = KonsensusApp(__name__)
app.config.update({'HDF5_REPO': '/W5/sade/workspace/hdf5_samples/uc1pc560.h5'})
app.run()