from pathlib import Path
from typing import Collection, Union

from headless_TFM.run_matlab_code import run_matlab_code

def manufacture_movies(template_path:Union[str,Path],data_paths:Collection[Union[str,Path]],input_paths:Collection[Union[str,Path]],output_paths:Collection[Union[str,Path]],run:bool=True,check_exist=True):
    if (len(data_paths) != len(input_paths) or len(input_paths) != len(output_paths)):
        raise ValueError("Must be same number of data paths, input paths, and output paths")

    decls = []
    for name,pathlist in ("data_paths",data_paths), ("input_paths",input_paths), ("output_paths",output_paths):
        if check_exist:
            paths = [Path(p) for p in pathlist]
# 
            # from IPython import embed; embed()
            if not all(p.exists() if name == "input_paths" else p.parent.exists() for p in paths): #we only care if the input folders themselves exist, everything else can be made after
                raise ValueError(f"Not all {name} exist on system! Please make the proper directories or set check_exist to false!");

            


        llist = ", ".join([f"'{path}'" for path in pathlist])
        code = f"{name} = {{{llist}}};"
        decls.append(code)

    func = f"movies = manufacture_movies('{template_path}',data_paths,input_paths,output_paths);"
    total_code = "".join(decls) + func

    code_res = run_matlab_code(total_code + "exit;",search_path=str(Path(__file__).absolute().parent)) if run else None

    return total_code, code_res

def make_movielist(list_path:Union[str,Path],list_out:Union[str,Path],
                   template_path:Union[str,Path],data_paths:Collection[Union[str,Path]],input_paths:Collection[Union[str,Path]],output_paths:Collection[Union[str,Path]],
                   run:bool=True, check_exist=True):
    
    #make the movies
    movies_code,_ = manufacture_movies(template_path,data_paths,input_paths,output_paths,run=False,check_exist=check_exist);
    
    if check_exist:
        if not Path(list_path).parent.exists():
            raise ValueError("MovieList save directory does not exist on system! Please make the proper directories or set check_exist to false!")
        if not Path(list_out).parent.exists():
            raise ValueError("MovieList output directory does not exist on system! Please make the proper directories or set check_exist to false!")

    #manufacture_movies puts result in movies variable, pass to make_movielist
    movielist_code = f"list = make_movielist('{list_path}','{list_out}',movies);"

    total_code = movies_code + movielist_code

    code_res = run_matlab_code(total_code + "exit;",search_path=str(Path(__file__).absolute().parent)) if run else None

    return total_code,code_res



if __name__ == "__main__":
    template = "tfm_demo_output/movieData.mat"
    folders = [Path(f"test/dum{i}") for i in range(20)]
    [f.mkdir(exist_ok=True,parents=True) for f in folders]
    saves = [f/"movDat.mat" for f in folders]
    ins = ["movie8_track9" for f in folders]
    # [i.mkdir(exist_ok=True) for i in ins]
    outs = [f/"out" for f in folders]

    movie_loc = "test/movielist.mat"
    movie_out = "test/movie_out"
    make_movielist(movie_loc,movie_out,template,saves,ins,outs)