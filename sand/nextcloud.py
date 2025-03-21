from core.fileutils import mdir, filegen
from core.static import interface
from core.uncompress import uncompress
from pathlib import Path
from os import system


sharelink_eoread = 'https://docs.hygeos.com/s/Fy2bYLpaxGncgPM/'


@interface()
def download_nextcloud(product_name: str, 
                       output_dir: Path | str, 
                       input_dir: Path | str = ''):
    """
    Function for downloading data from Nextcloud contained in the data/eoread directory

    Args:
        product_name (str): Name of the product with the extension
        output_dir (Path | str): Directory where to store downloaded data
        input_dir (Path | str, optional): Sub repository in which the product are stored. Defaults to ''.

    Returns:
        Path: Output path of the downloaded data
    """
    
    @filegen(arg=0)
    def download_with_filegen(output_path, input_path):
        system(f'wget {sharelink_eoread}/download?files={input_path} -c -O {output_path}')
    
    output_dir = mdir(output_dir)
    outpath    = output_dir/product_name
    inputpath  = Path(input_dir)/product_name
    
    download_with_filegen(outpath, inputpath)
    
    # Uncompress downloaded file 
    if product_name.split('.')[-1] in ['zip','gz']:
        return uncompress(outpath, output_dir)
        
    return outpath
    