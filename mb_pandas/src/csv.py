import pandas as pd
import tqdm

async def read_csv_asyn(filepath, show_progress=False, context_vars: dict = {}, **kwargs):

    def postprocess(df):
        # special treatment of fields introduced by function dfpack()
        for key in df:
            if key.endswith('_df_nd_ravel'):
                has_ndarray = True
                break
        else:
            has_ndarray = False
        if has_ndarray:
            df = df.copy() # to avoid generating a warning
            fromlist = lambda x: None if x is None else np.array(json.loads(x))
            for key in df:
                if key.endswith('_df_nd_ravel'):
                    df[key] = df[key].apply(fromlist)
                elif key.endswith('_df_nd_shape'):
                    df[key] = df[key].apply(fromlist)
        return df

    def process(filepath, data1: io.StringIO, data2, show_progress=False, **kwargs):

        # do read
        if show_progress:
            bar = tqdm(unit='row')
        dfs = []
        for df in pd.read_csv(data1, quoting=_csv.QUOTE_NONNUMERIC, chunksize=1024, **kwargs):
            dfs.append(df)
            if show_progress:
                bar.update(len(df))
        df = pd.concat(dfs, sort=False)
        if show_progress:
            bar.close()

        # If '.meta' file exists, assume general csv file and use pandas to read.
        if data2 is None: # no meta
            return postprocess(df)

        spinner = Halo(text="dfloading '{}'".format(filepath), spinner='dots') if show_progress else dummy_scope
        with spinner:
            try:
                # extract 'index_col' and 'dtype' from kwargs
                index_col = kwargs.pop('index_col', None)
                dtype = kwargs.pop('dtype', None)

                # load the metadata
                if show_progress:
                    spinner.text = "loading the metadata"
                meta = None if data2 is None else json.loads(data2)

                if meta is None:
                    if show_progress:
                        spinner.succeed("dfloaded '{}' with no metadata".format(filepath))
                    return postprocess(df)

                # From now on, meta takes priority over dtype. We will ignore dtype.
                kwargs['dtype'] = 'object'

                # update index_col if it does not exist and meta has it
                if index_col is None and len(meta['index_names']) > 0:
                    index_col = meta['index_names']

                # adjust the returning dataframe based on the given meta
                s = meta['columns']
                for x in s:
                    if show_progress:
                        spinner.text = "checking column '{}'".format(x)
                    y = s[x]
                    if y == 'datetime64[ns]':
                        df[x] = pd.to_datetime(df[x])
                    elif isinstance(y, list) and y[0] == 'category':
                        cat_dtype = pd.api.types.CategoricalDtype(categories=y[1], ordered=y[2])
                        df[x] = df[x].astype(cat_dtype)
                    elif y == 'int64':
                        df[x] = df[x].astype(np.int64)
                    elif y == 'uint8':
                        df[x] = df[x].astype(np.uint8)
                    elif y == 'float64':
                        df[x] = df[x].astype(np.float64)
                    elif y == 'bool':
                        # dd is very strict at reading a csv. It may read True as 'True' and False as 'False'.
                        df[x] = df[x].replace('True', True).replace(
                            'False', False).astype(np.bool)
                    elif y == 'object':
                        pass
                    else:
                        raise OSError("Unknown dtype for conversion {}".format(y))

                # set the index_col if it exists
                if index_col is not None and len(index_col) > 0:
                    df = df.set_index(index_col, drop=True)

                if show_progress:
                    spinner.succeed("dfloaded '{}'".format(filepath))
            except:
                if show_progress:
                    spinner.succeed("failed to dfload '{}'".format(filepath))
                raise

        return postprocess(df)

    # make sure we do not concurrently access the file
    with path.lock(filepath, to_write=False):
        if filepath.lower().endswith('.csv.zip'):
            data = await aio.read_binary(filepath, context_vars=context_vars)
            with _zf.ZipFile(io.BytesIO(data), mode='r') as myzip:
                filename = path.basename(filepath)[:-4]
                fp1 = myzip.open(filename, mode='r', force_zip64=True)
                data1 = fp1.read().decode()
                meta_filename = filename[:-4]+'.meta'
                if meta_filename in myzip.namelist():
                    data2 = myzip.open(meta_filename, mode='r').read()
                else:
                    data2 = None
                return process(filepath, data1, data2, show_progress=show_progress, **kwargs)
        else:
            fp1 = filepath
            data1 = await aio.read_text(fp1, context_vars=context_vars)
            meta_filepath = path.basename(filepath)[:-4]+'.meta'
            if path.exists(meta_filepath):
                data2 = await aio.read_text(meta_filepath, context_vars=context_vars)
            else:
                data2 = None
            return process(filepath, io.StringIO(data1), data2, show_progress=show_progress, **kwargs)