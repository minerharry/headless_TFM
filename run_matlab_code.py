import subprocess


matlab_path = "matlab" #might need absolute path
def run_matlab_code(code:str,exe_path=matlab_path,search_path=""):
    #only works pre-2019
    # return subprocess.run([exe_path,"-nodisplay","-nosplash","-nodesktop","-r",code])

    #only works post-2019
    
    res =  subprocess.run([exe_path,"-batch",code],capture_output=True,env={"MATLABPATH":search_path})
    if res.returncode != 0:
        raise MatlabError(res.stderr.decode())
    return res

    
class MatlabError(Exception):
    pass
