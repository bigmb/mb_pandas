import asyncio

__all__ = ['srun']

def srun(async_func, *args,extra_context_var: dict={} ,show_progress=False, **kwargs) -> object:
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
