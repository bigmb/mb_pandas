import asyncio
import aiofiles 

__all__ = ['srun','read_text']

def srun(async_func, *args,extra_context_var: dict={} ,show_progress=False, **kwargs):
    """
    Run asyncio function in synchronous way
    Input:
        func (function): function to run
        *args: arguments to pass to function
        extra_context_var (dict): extra variable to pass to function
        show_progress (bool): show progress bar
        **kwargs: keyword arguments to pass to function
    Output:
        result (object): result of function
    """
    try:
        context_vars = {}
        context_vars.update(extra_context_var)
        core = async_func(*args, context_vars,**kwargs)
        core.send(None)
        core.close()
    except StopIteration as e:
        return e.value


async def read_text(filepath, size: int = None, context_vars: dict = {}) -> str:
    """An asyn function that opens a text file and reads the content.
    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    Returns
    -------
    str
        the content read from file
    """

    if context_vars["async"]:
        async with aiofiles.open(filepath, mode="rt") as f:
            return await f.read(size)
    else:
        with open(filepath, mode="rt") as f:
            return f.read(size)

