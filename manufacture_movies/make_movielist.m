function m = make_movielist(save_loc,output_dir,movies)
arguments
    save_loc (1,1) string
    output_dir (1,1) string
    movies (1,:)
end

m = MovieList(movies,char(GetFullPath(output_dir)));
[dir,file,ext] = fileparts(save_loc);
if ext ~= '.mat'
    error("MovieList save location must be a .mat file!")
end
m.setPath(char(GetFullPath(dir)));
m.setFilename(char(file+ext))

m.save()

