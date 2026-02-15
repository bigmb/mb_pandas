"""
Asynchronous I/O utilities.

This module provides utilities for handling asynchronous file operations and running
async functions in a synchronous context.
"""

from typing import Any, Dict, Optional, Callable, TypeVar, Coroutine, Union
import asyncio
import aiofiles

__all__ = ['srun', 'read_text']

T = TypeVar('T')  # Generic type for function return value

def srun(async_func: Callable[..., Coroutine[Any, Any, T]], 
         *args: Any,
         extra_context_var: Optional[Dict[str, Any]] = None,
         show_progress: bool = False,
         **kwargs: Any) -> T:
    """
    Run an async function in a synchronous context.
    
    This function allows running asynchronous functions in a synchronous way by
    handling the async/await machinery internally.
    
    Args:
        async_func: The async function to run
        *args: Positional arguments to pass to the function
        extra_context_var: Additional context variables to pass to the function
        show_progress: Whether to show a progress bar
        **kwargs: Keyword arguments to pass to the function
        
    Returns:
        The result of the async function execution
    """
    context_vars = extra_context_var or {}
    
    async def _run_async():
        try:
            return await async_func(*args, context_vars=context_vars, **kwargs)
        except Exception as e:
            raise type(e)(f"Error in async function {async_func.__name__}: {str(e)}")
    
    return asyncio.run(_run_async())

async def read_text(filepath: str, 
                   size: Optional[int] = None, 
                   context_vars: Optional[Dict[str, Any]] = None) -> str:
    """
    Asynchronously read text from a file.
    
    This function provides both async and sync file reading capabilities based on
    the context_vars['async'] flag.
    
    Args:
        filepath: Path to the file to read
        size: Number of bytes to read from the beginning of the file.
              If None, reads the entire file.
        context_vars: Dictionary of context variables. Must include 'async' key
                     to determine whether to use async or sync reading.
                     
    Returns:
        str: Content read from the file
        
    Example:
        >>> # Async reading
        >>> content = await read_text('file.txt', context_vars={'async': True})
        >>> # Sync reading
        >>> content = await read_text('file.txt', context_vars={'async': False})
    """
    if not filepath:
        raise ValueError("filepath cannot be empty")
        
    context_vars = context_vars or {'async': True}
    
    if 'async' not in context_vars:
        raise KeyError("context_vars must contain 'async' key")
    
    try:
        if context_vars["async"]:
            async with aiofiles.open(filepath, mode="rt") as f:
                return await f.read(size)
        else:
            with open(filepath, mode="rt") as f:
                return f.read(size)
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {filepath}")
    except IOError as e:
        raise IOError(f"Error reading file {filepath}: {str(e)}")
    except Exception as e:
        raise type(e)(f"Unexpected error reading file {filepath}: {str(e)}")
