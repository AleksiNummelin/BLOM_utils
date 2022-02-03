import xarray as xr
import glob
import shutil
from netCDF4 import Dataset
from joblib import Parallel, delayed
#############################################
# PLEASE MODIFY BELOW DEPENDING ON YOUR NEEDS
# 
fpath = '/cluster/work/users/anu074/archive/'
cases = ['N1850frc2NOOBGC_f09_tnx0125v4_test4']
# here you can choose which components to convert
# atm, ocn, and ice are most important for storage
realms = ['atm','ocn','ice','lnd','rof']
# (1) n_jobs needs to match cpus-per-task in the sbatch
#     script. Overall the combination of cpus-per-task 
#     and memory depends on file size and somehow also on
#     the computational load on Betzy.
# (2) However, this script is completely robust against
#     file corruption, so one can try increasing
#     CPU's and reducing memory until the job crashes
#     without the fear of data corruption. Usually
#     8 cpu's and 2GB memory is enough for atm
#     but 4 cpu's and 4 GB is needed for the ocean.
# (3) The most efficient is to convert in steps
#     for example, submitting conversion for
#     atm, ocean, and ice as separate jobs
n_jobs = 8
#
##########################################
#
def netcdf3to4_parallel(fname):
    print(fname)
    f = Dataset(fname)
    if f.file_format not in ['NETCDF4']: #convert if not a netcdf4 already
        f.close()
        data = xr.open_dataset(fname,chunks={})
        glbl_attrs = data.attrs #preserve attributes
        # save memory by writing one variable at the time
        variables = list(set(list(data.variables))-set(list(data.coords))-set(list(data.dims)))
        for v,var in enumerate(variables):
            # rechunk if the dims is large than 1.
            # here we split each dimension by 5, but that could be tuned
            #print(var)
            varshape = data[var].shape
            if len(varshape)>1:
                newshape = (max(1,round(varshape[0]/5)),)
                for s in varshape[1:]:
                    newshape=newshape+(max(1,round(s/5)),)
                new_encoding = {var: {'complevel': 5, 'shuffle': True, 'zlib': True,'contiguous':False,'chunksizes':newshape,'original_shape':varshape}}
            else:
                newshape = varshape
                new_encoding = {var: {'complevel': 5, 'shuffle': True, 'zlib': True,'contiguous':False}}
            if v==0:
                data0=data[var].to_dataset()
                data0.attrs=glbl_attrs # write attributes
                if 'time' in data[var].dims:
                    data0.to_netcdf(fname+'_tmp',mode='w',format='NETCDF4',encoding=new_encoding,unlimited_dims=['time'])
                else:
                    data0.to_netcdf(fname+'_tmp',mode='w',format='NETCDF4',encoding=new_encoding)
            else:
                data1=data[var].to_dataset()
                #
                if 'time' in data[var].dims:
                    data1.to_netcdf(fname+'_tmp',mode='a',format='NETCDF4',encoding=new_encoding,unlimited_dims=['time'])
                else:
                    data1.to_netcdf(fname+'_tmp',mode='a',format='NETCDF4',encoding=new_encoding)
                #
        data.close()
        shutil.move(fname+'_tmp',fname)
    else:
        f.close()

# parallel loop
for case in cases:
    for realm in realms:
        fnames = sorted(glob.glob(fpath+case+'/'+realm+'/hist/*.nc'))
        if realm in ['atm']:
            fnames2=fnames.copy()
            for fname in fnames:
                if '.cam.i.' in fname: #let's not mess with the initial condition files, although these should be copied to rest as well I think
                    fnames2.pop(fnames2.index(fname))
            fnames=fnames2.copy()
        print(fnames)
        Parallel(n_jobs=n_jobs)(delayed(netcdf3to4_parallel)(fname) for fname in fnames)
