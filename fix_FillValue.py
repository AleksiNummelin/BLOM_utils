import xarray as xr
import glob
import shutil
from joblib import Parallel, delayed
#
fpath = '/cluster/work/users/anu074/archive/'
cases = ['NOIIAJRAOC20TR_TL319_tn14_20211214_2DdiffAnisot3D_EGC2EGIDFQ1']
realms = ['atm','ocn','ice','lnd','rof']
FillValues = {'atm':1.E36,'ocn':9.96920996838687E36,'ice':1.E30,'lnd':1.E36,'rof':1.E36}
#
n_jobs = 8
#
def fix_FillValue(fname,FillValue):
    """Set time to be unlimited"""
    data = xr.open_dataset(fname,chunks={})
    data_encoding={}
    variables = list(set(list(data.variables))-set(list(data.coords))-set(list(data.dims)))
    for var in variables:
        var_encoding=data[var].encoding
        var_encoding['_FillValue']=FillValue
        data_encoding[var]=var_encoding
    data.to_netcdf(fname+'_tmp',format='NETCDF4',encoding=data_encoding,unlimited_dims=['time'])
    data.close()
    shutil.move(fname+'_tmp',fname)

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
        Parallel(n_jobs=n_jobs)(delayed(fix_FillValue)(fname,FillValues[realm]) for fname in fnames)
