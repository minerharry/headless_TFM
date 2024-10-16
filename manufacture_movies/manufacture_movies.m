function paths = manufacture_movies(template,movpath,inpath,outpath,copy_processes)
arguments
   template (1,1) string 
    movpath (1,:) string
    inpath (1,:) string
    outpath (1,:) string
    copy_processes (1,1) logical = true
end

if (length(movpath) ~= length(inpath) || length(inpath) ~= length(outpath))
    error("movpath, inpath, and outpath must all be the same size!")
end

paths = strings(1,length(movpath));

MD = load(template).MD;
%load all nonempty data except channels (input) and output directory
template_data = remove_empty_kwargs(remove_kwarg(unpack(MD),...
    'channels_','outputDirectory_','movieDataPath_','movieDataFileName_',...
    'nFrames_', 'imSize_', 'zSize_','processes_','packages_' ...
));

template_channel = MD.channels_;

orig_in = template_channel.channelPath_;
orig_out = MD.outputDirectory_;

function res = isChild(p1,p2)
   %%is p1 a child of p2
   between = Path(p1).relative(p2);
   %if you can get to p1 via p2 without going up any directories, p1 is
   %a child of p2
   res = all(between.parts ~= ".."); 
end

%if the orig in is inside of orig_out, we need to make sure we do it first
in_first = isChild(orig_in,orig_out); 

function relocate(obj,new_in,new_out)
    if in_first
       obj.relocate(orig_in,new_in)
    end
    obj.relocate(orig_out,new_out)
    if ~in_first
        obj.relocate(orig_in,new_in)
    end
end
    

for i=1:length(inpath)
    mov = movpath(i);
    in = char(GetFullPath(inpath(i)));
    out = char(GetFullPath(outpath(i)));
    
    newchannel = change_channel_path(template_channel,in);
    
    newMov = MovieData(newchannel,template_data{:});
    status = mkdir(out);
    newMov.outputDirectory_ = out;
    
    [dir,name,ext] = fileparts(GetFullPath(mov));
    if (ext ~= ".mat") %more likely specifying a dir than a file with no extension
        error("movie path must specify a .mat file!");
    end
    
    newMov.movieDataPath_ = char(dir);
    newMov.movieDataFileName_ = char(name + ext);
    
    if (copy_processes)
        for j=1:length(MD.packages_)
            pack = MD.packages_{j};
            newpack = TFMPackage(newMov,out);
            
            if (class(pack) ~= "TFMPackage")
                error("unrecognized package: " + pack)
            end
            
            for k=1:length(pack.processes_)
               proc = pack.processes_{k};

               %danuser lab: real one for this. no feval shenanigans
               %needed!
               constructor = pack.getDefaultProcessConstructors{k};
               %all package constructors have syntax {owner, outputdir,
               %function params}. We want to keep function params and
               %replace owner and outputdir
               newproc = constructor(newMov, out, proc.funParams_);
               
               relocate(newproc,in,out) %fix any remaining paths from proc.funParams_
               
               newpack.setProcess(k,newproc)
               newMov.addProcess(newproc)
            end
            newMov.addPackage(newpack)
        end
        
    end
    
    save(newMov)
    
    paths(i) = GetFullPath(mov);
end

paths = cellstr(paths);


end
