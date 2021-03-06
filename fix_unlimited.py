import xarray as xr
import glob
import shutil
from joblib import Parallel, delayed
#
fpath = '/cluster/work/users/anu074/archive/'
cases = ['NOIIAJRAOC20TR_TL319_tn14_20220110_2Ddiff_EGC2EGIDFQ1']
realms = ['atm','ocn','ice','lnd','rof']
#
n_jobs = 8
#
def fix_unlimited_time(fname):
    """Set time to be unlimited"""
    data = xr.open_dataset(fname,chunks={})
    data.to_netcdf(fname+'_tmp',format='NETCDF4',unlimited_dims=['time'])
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
        Parallel(n_jobs=n_jobs)(delayed(fix_unlimited_time)(fname) for fname in fnames)
