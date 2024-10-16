from enum import Enum
from numbers import Real
from pathlib import Path
from typing import Any, Iterable

from run_matlab_code import run_matlab_code


class BatchType(Enum):
    Client = "BatchType.Client"
    Single = "BatchType.Single"
    One_Per_Movie = "BatchType.One_Per_Movie"


def process_movielist(movielist:str|Path,
                      procs_to_run:Iterable[int],
                      parallel:bool|None=True,
                      force_run:bool|None=True,
                      package:str|None='TFMPackage',
                      batchType:str|BatchType|None=BatchType.Client,
                      poolSize:int|None=None,
                      clusterProfile:str|None=None,
                      run:bool=True):
    
    load_list = f"ML = load('{movielist}').ML"
    load_procs = f"procs_to_run = [{' '.join(map(str,procs_to_run))}]"

    def setvar(varname:str,val:Any)->str:
        if val is None:
            return ""
        res = f"{varname} = "
        if isinstance(val,bool):
            res += "true" if val else "false"
        elif isinstance(val,str):
            res += f"'{val}'"
        elif isinstance(val,Enum):
            res += str(val.value)
        elif isinstance(val,Real):
            res += f"{val}"
        else:
            raise TypeError
        return res

    if isinstance(batchType,str):
        batchType = BatchType._member_map_[batchType]

    load_parallel = setvar("parallel",parallel) #parallel is optional, not keyword, so it gets its own thing

    #group all the keyword arguments together to construct input
    #string names *need* to have the same name as the matlab kwarg
    kwarg_keys:list[tuple[str,Any]] = [ 
        ("force_run",force_run),
        ("packageName",package),
        ("batchType",batchType),
        ("poolSize",poolSize),
        ("clusterProfile",clusterProfile),
    ]

    load_kwargs = ";".join(map(setvar,*zip(*kwarg_keys)))

    def getkwarg(varname:str,val:Any)->str:
        if val is None:
            return ""
        else:
            return f",'{varname}',{varname}"

    kwargs = "".join(map(getkwarg,*zip(*kwarg_keys)))

    run_movies = f"exceptions = process_movies_headless(ML,procs_to_run{',parallel' if parallel is not None else ''}{kwargs})"

    total_code = load_list + ";" + load_procs + ";" + load_parallel + ";" + load_kwargs + ";" + run_movies + ";"

    code_res = run_matlab_code(total_code + "exit;",search_path=str(Path(__file__).absolute().parent)) if run else None

    return total_code, code_res


if __name__ == "__main__":
    template_list = "test/movielist.mat"
    process_movielist(template_list,range(1,6),parallel=True,poolSize=6)

    
