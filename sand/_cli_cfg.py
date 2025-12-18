from core.log import rgb

class SearchCfg:
    colors = dict(
        Name        = rgb.default,
        level       = rgb.orange,
        provider    = rgb.red,
        resolution  = rgb.purple,
        swath       = rgb.cyan, 
        longname    = rgb.gray,
        orbit       = rgb.cyan,
        cycle       = rgb.green,
        launch_date = rgb.blue,
        end_date    = rgb.blue,
    )