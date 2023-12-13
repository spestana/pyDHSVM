import xarray as xr
from glob import glob
import zarr
import os
import shutil
import rioxarray
from dask.distributed import Client, LocalCluster

# from https://github.com/friedrichknuth/gtsa/blob/main/gtsa/io.py
def dask_start_cluster(
    workers,
    threads=1,
    ip_address=None,
    port=":8787",
    open_browser=False,
    verbose=True,
):
    """
    Starts a dask cluster. Can provide a custom IP or URL to view the progress dashboard.
    This may be necessary if working on a remote machine.
    """
    cluster = LocalCluster(
        n_workers=workers,
        threads_per_worker=threads,
        #silence_logs=logging.ERROR,
        dashboard_address=port,
    )

    client = Client(cluster)

    if ip_address:
        if ip_address[-1] == "/":
            ip_address = ip_address[:-1]  # remove trailing '/' in case it exists
        port = str(cluster.dashboard_link.split(":")[-1])
        url = ":".join([ip_address, port])
        if verbose:
            print("\n" + "Dask dashboard at:", url)
    else:
        if verbose:
            print("\n" + "Dask dashboard at:", cluster.dashboard_link)
        url = cluster.dashboard_link

    if port not in url:
        if verbose:
            print("Port", port, "already occupied")

    if verbose:
        print("Workers:", workers)
        print("Threads per worker:", threads, "\n")

    if open_browser:
        webbrowser.open(url, new=0, autoraise=True)

    return client



def forcing_to_netcdf(input_path: str, output_path: str) -> None:
    """
    Converts a DHSVM forcing data text file to a NetCDF file
    
    Parameters
    ------------
    input_path: str 
        path to the DHSVM forcing data text file
    output_path: str
        path and filename to save the resulting NetCDF file

    Returns
    -----------
    None
    """

    # column names for DHSVM forcing data
    names = ['time', 'tair', 'wind', 'rh', 'sw', 'lw', 'precip']

    # open file, set column names
    df = pd.read_csv(input_path, names=names, sep='\\s+')
    # set time column as index
    df = df.set_index(pd.DatetimeIndex(df.time))

    # get lat and lon from filename
    lat = float(filepath.split('/')[-1].split('_')[1])
    lon = float(filepath.split('/')[-1].split('_')[2])

    # turn the dataframe into an xarray dataset, assign lon and lat coords their values and expand the dims so this has 3 dimensions (time, lon, lat)
    ds = df.to_xarray().assign_coords({'lat': lat, 'lon': lon}).expand_dims(['lat', 'lon'])
    # save out dataset to a new netcdf file
    ds.to_netcdf(output_path)

    

def batch_forcing_to_netcdf(input_dir: str, output_dir: str, fn_prefix: str='data') -> None:
    """
    Batch converts DHSVM forcing data text files to NetCDF files
    
    Parameters
    ------------
    input_dir: str 
        path to directory containing DHSVM forcing data text files
    output_dir: str
        path to directory to save the resulting NetCDF files
    fn_prefix: str
        prefix on DHSVM forcing data text files

    Returns
    -----------
    None
    """
    
    # get list of all the forcing data files from this location
    data_list = glob(input_dir + f'/{fn_prefix}*')

    # read each file and convert to a netcdf
    for i, filepath in enumerate(data_list):
        print(f"Processing {i+1} of {len(data_list)}...", end="\r")
        output_path = output_dir + filepath.split('/')[-1] + '.nc'
        if not os.path.exists(new_file_name):
            try:
                forcing_to_netcdf(input_path: str, output_path: str)
            except Exception as err:
                print(f"Failed on {filepath}")
                print(f"Error: {err}")

    

def forcing_to_zarr(input_dir: str, zarr_output_path: str, fn_prefix: str='data') --> None:    
    """
    Compiles DHSVM forcing data NetCDF files into a single zarr file
    
    Parameters
    ------------
    input_dir: str 
        path to directory containing DHSVM forcing data NetCDF files
    zarr_output_path: str
        path and filename to save the resulting zarr file
    fn_prefix: str
        prefix on DHSVM forcing data NetCDF files

    Returns
    -----------
    None
    """    
    
    # find all the netcdf files we just made
    data_list = glob(input_dir + f'/{fn_prefix}*')
    
    #open all of the netcdf files and compile, save as zarr file
    with dask_start_cluster(
                                workers=6,
                                threads=2,
                                ip_address='http://dshydro.ce.washington.edu',
                                port=":8786",
                                open_browser=False,
                                verbose=True,
                                ) as client:
        
        # Open multifile dataset
        # Open all the raster files as a single dataset (combining them together)
        # Why did we choose chunks = 500? 100MB?
        # https://docs.xarray.dev/en/stable/user-guide/dask.html#optimization-tips
        ds = xr.open_mfdataset(data_list, chunks={'time': 'auto'}) #preprocess=fix_grid  combine='nested', concat_dim='time',
        ds['time'] = pd.DatetimeIndex(ds['time'].values)
        # add CRS info
        ds = ds.rio.write_crs('epsg:4326')

        # Create an output zarr file and write these chunks to disk
        # if already exists, remove it here
        if os.path.exists(zarr_output_path):
            shutil.rmtree(zarr_output_path, ignore_errors=False)

        # save final file out as a zarr
        ds.to_zarr(zarr_output_path)



