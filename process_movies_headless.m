function [movieException] = process_movies_headless(movies,procs_to_run,varargin)

ip = inputParser;
ip.addRequired('movies',@(x) isa(x,'MovieObject') || (isa(x,'cell') && all(cellfun(@(c) isa(c,'MovieObject'),x))));
ip.addRequired('procs_to_run',@isnumeric);
ip.addOptional('parallel','auto',@(x) islogical(x) || (ischar(x) && strcmp(x,'auto')))
ip.addParameter('force_run',false,@islogical);
ip.addParameter('packageName','TFMPackage',@ischar);
ip.addParameter('batchType',BatchType.Client,@(x) isa(x,'BatchType'));
ip.addParameter('poolSize',nan,@isnumeric);
ip.addParameter('clusterProfile','local',@ischar);

ip.parse(movies,procs_to_run,varargin{:})

force_run = ip.Results.force_run;
packageName = ip.Results.packageName;
batchType = ip.Results.batchType;
poolSize = ip.Results.poolSize;
do_parallel = ip.Results.parallel;
clusterProfile = ip.Results.clusterProfile;


%% headless version of packageGUI_RunFcn

%as with much in this file, we're going to be zombifying the "userData"
%struct. The following fields are necessary:

%- package: most important. 1xN array of packages for each movie to process
%- id: index of the current package in packages.
%- crtPackage: must equal userData.package(id)

if isa(movies,'MovieList')
    movies = movies.getMovies();
end

userData = struct();

userData.nMovies = length(movies);

if isa(movies,'cell')
    movies = cellfun(@(x) x,movies); 
end

% populate packages: get the specified package from each movie
userData.package = arrayfun(@(movie) movie.getPackage(movie.getPackageIndex(packageName,1,false)),movies);

%set procs to run; since we're processing all movies, they all get the same
%values
movieProcs = arrayfun(@(x) procs_to_run, (1:userData.nMovies), 'UniformOutput',false);

% since we don't have gui or selection, we're going to presume the user has
% full control of the inputs of this function. As such, while the
% single-movie stuff will be left as vestigial code, we will attempt to run
% all movies (userData.id set to 1 to start)
userData.id = 1;
userData.crtPackage = userData.package(userData.id);

% Again, we're just gonna run all the movies
if true
    movieList = circshift(1:userData.nMovies,[0 -(userData.id-1)]);
else
    movieList=userData.id;
end


procCheck=cell(1,userData.nMovies);
procCheck(movieList)=arrayfun(@(x) movieProcs{x},movieList,...
    'UniformOutput',false);

hasValidProc = arrayfun(@(x) any(procCheck{x}),movieList);
movieRun=movieList(hasValidProc);
procCheck = procCheck(hasValidProc);


movieException = cell(1, userData.nMovies);
procRun = cell(1, userData.nMovies);

%check for unpopulated processes
isProcSet=@(x,y)~isempty(userData.package(x).processes_{y});
isMovieProcSet = @(x) all(arrayfun(@(y)isProcSet(x,y),procCheck{x}));
invalidMovies=movieRun(~arrayfun(isMovieProcSet,movieRun));
for i = invalidMovies
    invalidProc = procCheck{i}(arrayfun(@(y)~isProcSet(i,y),procCheck{i}));
    for j=invalidProc
        ME = MException('lccb:run:setup', ['Step %d : %s is not set up yet.\n'...
            '\nTip: when step is set up successfully, the step name becomes bold.'],j,...
            eval([userData.package(i).getProcessClassNames{j} '.getName']));
        movieException{i} = horzcat(movieException{i}, ME);
    end
end

validMovies=movieRun(arrayfun(isMovieProcSet,movieRun));
for iMovie = validMovies   
    % Check if selected processes have alrady be successfully run
    % If force run, re-run every process that is checked
    if ~force_run
        
        k = true;
        for i = procCheck{iMovie}
            
            if  ~( userData.package(iMovie).processes_{i}.success_ && ...
                    ~userData.package(iMovie).processes_{i}.procChanged_ ) || ...
                    ~userData.package(iMovie).processes_{i}.updated_
                
                k = false;
                procRun{iMovie} = horzcat(procRun{iMovie}, i);
            end
        end
        if k
            movieRun = setdiff(movieRun, iMovie);
            continue
        end
    else
        procRun{iMovie} = procCheck{iMovie};
    end    
    
    % Package full sanity check. Sanitycheck every checked process
    [status procEx] = userData.package(iMovie).sanityCheck(true, procRun{iMovie});
    
    invalidProcEx = procRun{iMovie}(~cellfun(@isempty,procEx(procRun{iMovie})));
    for i = invalidProcEx
        % Check if there is fatal error in exception array
        if strcmp(procEx{i}(1).identifier, 'lccb:set:fatal') || ...
                strcmp(procEx{i}(1).identifier, 'lccb:input:fatal')

            ME = MException('lccb:run:sanitycheck','Step %d %s: \n%s',...
                i,userData.package(iMovie).processes_{i}.getName, procEx{i}(1).message);
            movieException{iMovie} = horzcat(movieException{iMovie}, ME);
                
        end
    end
end


if (strcmp(do_parallel,'auto'))
    do_parallel = ~isempty(uTrackParCluster);
end

if (do_parallel)
    if isempty(uTrackParCluster) || ~strcmp(uTrackParCluster().Profile,clusterProfile)
        uTrackParCluster(clusterProfile)
    end
    
    %% parallel settings
    parSettings = struct('batch',batchType,'poolSize',poolSize);
    movieException = start_processing_movies_in_parallel(movieRun,userData,parSettings, movieException,procRun);
else
    movieException = start_processing_movies_in_series(movieRun,userData,movieException,procRun);
end
end


function [movieException] = start_processing_movies_in_series(movieRun,userData,movieException, procRun)
% start_processing_movies_in_series Run legacy code for single thread
% processing

for i=1:length(movieRun)
    iMovie = movieRun(i);
   
    if iMovie ~= userData.id
        userData = switchMovie(userData,iMovie);
    end
    
    % Run algorithms!
    try
        
        for procID = procRun{iMovie}
            userfcn_runProc_dfs(procID, procRun{iMovie}, userData); % user data is retrieved, updated and submitted
        end
        
    catch ME
        
        % Save the error into movie Exception cell array
        ME2 = MException('lccb:run:error','Step %d: %s',...
            procID,userData.package(iMovie).processes_{procID}.getName);
        movieException{iMovie} = horzcat(movieException{iMovie}, ME2);
        movieException{iMovie}=movieException{iMovie}.addCause(ME);
        
        procRun{iMovie} = procRun{iMovie}(procRun{iMovie} < procID);
    end
end
end

%%parallel functions: in the GUI version, there are a lot of settings
%%stored in the parallel dropdown menu. First thing it does is load (most,
%%but not all!) of the GUI stuff into the parSettings struct. I changed a
%%bit of the meaning, though, be warned!

% struct - parSettings
% batch: BatchType
% poolSize: int (or nan)

function movieException = start_processing_movies_in_parallel(movieRun,userData,parSettings,movieException, procRun)
% start_processing_movies_in_parallel New code for parallel processing on
% the MovieData level
    
    switch(parSettings.batch)
        case BatchType.Client % This Client
            if(~isnan(parSettings.poolSize))
                start_processing_movies_in_parallel_parfeval(movieRun,userData,parSettings,movieException, procRun);
            else
                error('packageGUI_RunFcn:parallel:NotPossible', ...
                    'For running from this client, you must select a pool size');
            end
        case BatchType.Single % Single Batch Job
            start_processing_movies_in_parallel_single_batch(movieRun,userData,parSettings,movieException, procRun);
        case BatchType.One_Per_Movie % One Per Movie
            start_processing_movies_in_parallel_batch_per_movie(movieRun,userData,parSettings,movieException, procRun)
        otherwise
            error('packageGUI_RunFcn:parallel:UnknownBatchSetting', ...
                'Could not determine parallel batch setting for uTrack')
    end
    
end

function movieException = start_processing_movies_in_parallel_parfeval(movieRun,userData,parSettings,movieException, procRun)
    
    currentPool = gcp('nocreate');

    if(~isempty(currentPool))
        if(currentPool.NumWorkers ~= parSettings.poolSize ...
                || ~strcmp(get(uTrackParCluster,'Profile'), ...
                   currentPool.Cluster.Profile))
            delete(currentPool);
            currentPool = [];
        end
    end
    if(isempty(currentPool))
        currentPool = parpool(uTrackParCluster,parSettings.poolSize);
    end

    for i=1:length(movieRun)
        iMovie = movieRun(i);

        userData = switchMovie(userData, iMovie);
        
        % Determine sequence that parent processes and checked processes
        % should be run
        procSequence = userData.crtPackage.getProcessSequence(procRun{iMovie});
        % Determine which processes were successful
        procSuccess =  cellfun(@(proc) proc.success_,userData.crtPackage.processes_(procSequence));
        % Only run the processes that were either requested or were not
        % successful
        mustRun = ismember(procSequence,procRun{iMovie});
        procSequence = procSequence(~procSuccess | mustRun);
        procs = userData.crtPackage.processes_(procSequence);
        % Save procSequence for reload
        procRun{iMovie} = procSequence;
%         f = @(p,~) cellfun(@run,p);
        jobs(i) = parfeval(currentPool,@runProcesses,1,procs,0);
       
    end

    
    % TODO: Do something better with this
    finishedJobs = zeros(size(jobs));
    finishedJobCount = 0;
    while(finishedJobCount ~= length(jobs))
        for i=1:length(jobs)
            if(~strcmp(jobs(i).State,'pending'))
                if(~finishedJobs(i))
                    if(wait(jobs(i),'finished',1))
                        finishedJobs(i) = true;
                        finishedJobCount = sum(finishedJobs);
                        
                        % Update Processes
                        newProcs = fetchOutputs(jobs(i));
                        iMovie = movieRun(i);
                        userData = updateUserData(userData, newProcs(1), iMovie);
                        
                        
                        fprintf('Movie %d output:\n',movieRun(i));
                        disp(jobs(i).Diary);
                        fprintf('\n');
                    elseif(strcmp(jobs(i).State,'failed'))
                        finishedJobs(i) = true;
                        finishedJobCount = sum(finishedJobs);
                        % Save the error into movie Exception cell array
                        ME = jobs(i).Error;
                        ME2 = MException('lccb:run:error','Parallel Run: Movie %d',...
                            i);
                        movieException{iMovie} = horzcat(movieException{iMovie}, ME2);
                        movieException{iMovie}=movieException{iMovie}.addCause(ME);

                        procRun{iMovie} = procRun{iMovie}(procRun{iMovie} < procID);

                    end
                end
            end
        end
    end
    for i=1:length(jobs)
        delete(jobs(i));
    end
end

function movieException = start_processing_movies_in_parallel_single_batch(movieRun,userData,parSettings,movieException, procRun)
    % Process all movies in a single batch job
    
    poolParams = {};
    if(~isnan(parSettings.poolSize))
        poolParams = {'Pool',parSettings.poolSize};
    end
    
    procsPerMovie = cell(size(movieRun));
    
    for i=1:length(movieRun)
        iMovie = movieRun(i);
        
        userData = switchMovie(userData, iMovie);
        
        % Determine sequence that parent processes and checked processes
        % should be run
        procSequence = userData.crtPackage.getProcessSequence(procRun{iMovie});
        % Determine which processes were successful
        procSuccess =  cellfun(@(proc) proc.success_,userData.crtPackage.processes_(procSequence));
        % Only run the processes that were either requested or were not
        % successful
        mustRun = ismember(procSequence,procRun{iMovie});
        procSequence = procSequence(~procSuccess | mustRun);
        procs = userData.crtPackage.processes_(procSequence);
        procsPerMovie{i} = procs;
    end

    
    job = batch(uTrackParCluster,@start_processing_movies_in_parallel_single_batch_job,2,{movieRun,procsPerMovie,movieException},'AutoAttachFiles',false,'CaptureDiary',true,poolParams{:});
    disp(job);
    wait(job);
    out = fetchOutputs(job);
    movieException = out{1};
    
    % Update Processes
    procs = out{2};
    for i=1:length(movieRun)
        % Update Processes
        iMovie = movieRun(i);
        userData = updateUserData(userData, procs{i}(1), iMovie);
    end
    
    job.diary;
    delete(job);
    

end

function [movieException,procs] = start_processing_movies_in_parallel_single_batch_job(movieRun,procsPerMovie,movieException)
    currentPool = gcp('nocreate');
    procs = cell(1,length(movieRun));
    if(isempty(currentPool))
        % No parallel pool available, run in serial
        for i=1:length(movieRun)
            iMovie = movieRun{i};
                       
            fprintf('Movie %d\n',movieRun(i));
            try
                procs{i} = runProcesses(procsPerMovie{i});
            catch ME
                ME2 = MException('lccb:run:error','Parallel Run: Movie %d',...
                    i);
                movieException{iMovie} = horzcat(movieException{iMovie}, ME2);
                movieException{iMovie}=movieException{iMovie}.addCause(ME);

%                 procRun{iMovie} = procRun{iMovie}(procRun{iMovie} < procID);
            end
        end
    else
        % Parallel pool available, use it!
        
        % Simpler parfor version
%         exceptions = movieException(movieRun);
%         parfor i=1:length(movieRun)
% %             iMovie = movieRun{i};
%             fprintf('Movie %d\n',movieRun(i));
%             try
%                 runProcesses(procsPerMovie{i});
%             catch ME
%                 ME2 = MException('lccb:run:error','Parallel Run: Movie %d',...
%                     i);
%                 exceptions{i} = horzcat(exceptions{i}, ME2);
%                 exceptions{i}= exceptions{i}.addCause(ME);
% 
% %                 procRun{iMovie} = procRun{iMovie}(procRun{iMovie} < procID);
%             end
%         end
%         movieException(movieRun) = exceptions;
        
        % Copied from pool version
        % TODO: Unify code
        currentPool = gcp;
        for i=1:length(movieRun)
            iMovie = movieRun(i);
            jobs(i) = parfeval(currentPool,@runProcesses,1,procsPerMovie{i},0);
        fprintf('Queuing %d of %d movies total ...\n', i, length(movieRun));
        end
        fprintf('Waiting for parallel job %d of %d movies total ...\n', 0, length(movieRun));

        % TODO: Do something better with this
        finishedJobs = zeros(size(jobs));
        finishedJobCount = 0;
        while(finishedJobCount ~= length(jobs))
            for i=1:length(jobs)
                if(~strcmp(jobs(i).State,'pending'))
                    if(~finishedJobs(i))
                        if(wait(jobs(i),'finished',1))
                            finishedJobs(i) = true;
                            finishedJobCount = sum(finishedJobs);
                            
                            % Fetch proceses to update later
                            newProcs = fetchOutputs(jobs(i));
                            procs{i} = newProcs{1};
                            
                            fprintf('Finished parallel job %d of %d\n',finishedJobCount,length(jobs))
                            fprintf('Movie %d output:\n',movieRun(i));
                            disp(jobs(i).Diary);
                            fprintf('\n');                        elseif(strcmp(jobs(i).State,'failed'))
                            finishedJobs(i) = true;
                            finishedJobCount = sum(finishedJobs);
                            % Save the error into movie Exception cell array
                            ME = jobs(i).Error;
                            ME2 = MException('lccb:run:error','Parallel Run: Movie %d',...
                                i);
                            movieException{iMovie} = horzcat(movieException{iMovie}, ME2);
                            movieException{iMovie}=movieException{iMovie}.addCause(ME);
                        end
                    end
                end
            end
        end
        for i=1:length(jobs)
            delete(jobs(i));
        end

    end
end

function userData = switchMovie(userData,newMovieId)
% selectMovie Selects the movie of interest, updating GUI and userData
% Importantly also sets userData.crtPackage via switchMovie_Callback
    if newMovieId ~= userData.id
        nMovies = userData.nMovies;
        userData.id = mod(newMovieId-1,nMovies)+1;
        userData.crtPackage = userData.package(userData.id);
    end
end


% Set up new movie GUI parameters


function movieException = start_processing_movies_in_parallel_batch_per_movie(movieRun,userData,parSettings,movieException, procRun)
    % Process each movie in a separate batch job
    
    poolParams = {};
    if(~isnan(parSettings.poolSize))
        poolParams = {'Pool',parSettings.poolSize};
    end

    for i=1:length(movieRun)
        iMovie = movieRun(i);
        
        userData = switchMovie(userData,iMovie);
        
        % Determine sequence that parent processes and checked processes
        % should be run
        procSequence = userData.crtPackage.getProcessSequence(procRun{iMovie});
        % Determine which processes were successful
        procSuccess =  cellfun(@(proc) proc.success_,userData.crtPackage.processes_(procSequence));
        % Only run the processes that were either requested or were not
        % successful
        mustRun = ismember(procSequence,procRun{iMovie});
        procSequence = procSequence(~procSuccess | mustRun);
        procs = userData.crtPackage.processes_(procSequence);
%         f = @(p,~) cellfun(@run,p);
%         jobs(i) = parfeval(gcp,f,0,procs,0);
        jobs(i) = batch(uTrackParCluster,@runProcesses,1,{procs,0},'AutoAttachFiles',false,'CaptureDiary',true,poolParams{:});
        procRun{iMovie} = procSequence;

    end


    % TODO: Do something better with this
    finishedJobs = zeros(size(jobs));
    finishedJobCount = 0;
    while(finishedJobCount ~= length(jobs))
        for i=1:length(jobs)
            if(~strcmp(jobs(i).State,'pending'))
                if(~finishedJobs(i))
                    if(wait(jobs(i),'finished',1))
                        finishedJobs(i) = true;
                        finishedJobCount = sum(finishedJobs);
                        
                        % Update Processes
                        newProcs = fetchOutputs(jobs(i));
                        newProcs = newProcs{1};
                        iMovie = movieRun(i);
                        userData = switchMovie(userData,userData);
                        userData = updateUserData(userData, newProcs(1), iMovie);
                        
                        fprintf('Movie %d output:\n',movieRun(i));
                        jobs(i).diary;
                        fprintf('\n');                    elseif(strcmp(jobs(i).State,'failed'))
                        finishedJobs(i) = true;
                        finishedJobCount = sum(finishedJobs);
                        % Save the error into movie Exception cell array
                        ME = jobs(i).Error;
                        ME2 = MException('lccb:run:error','Parallel Run: Movie %d',...
                            i);
                        movieException{iMovie} = horzcat(movieException{iMovie}, ME2);
                        movieException{iMovie}=movieException{iMovie}.addCause(ME);

                        procRun{iMovie} = procRun{iMovie}(procRun{iMovie} < procID);

                    end
                end
            end
        end
    end
    for i=1:length(jobs)
        delete(jobs(i));
    end
end


function userfcn_runProc_dfs (procID, procRun, userData)  % throws exception

parentRun = [];
parentID=userData.crtPackage.getParent(procID);

% if current process procID have dependency processes    
for j = parentID
    % if parent process is one of the processes need to be run
    % if parent process has already run successfully
    if any(j == procRun) && ~userData.crtPackage.processes_{j}.success_
        parentRun = horzcat(parentRun,j); %#ok<AGROW>
    end
end

% if above assumptions are yes, recursively run parent process' dfs fcn
for j = parentRun
    userfcn_runProc_dfs (j, procRun, userData)
end

try
    userData.crtPackage.processes_{procID}.run(); % throws exception
catch ME
    rethrow(ME)
end

end

function userData = updateUserData(userData,newProcs,iMovie)
%updateUserData update userData to replace all altered MovieData and Package handles
% If Process is run remotely, local handles may not be updated. The only way to update
% all these handles is to alter userData
    assert(length(newProcs) == length(iMovie), ...
        'packageGUI_RunFcn:updateUserData:argLength', ...
        'Arguments must be of the same length');
    
    if(isa(newProcs,'Process'))
        newProcs = {newProcs};
    end

    % Determine field
    if ~isempty(userData.MD), field='MD'; else field = 'ML'; end

    for i=1:length(newProcs)
        proc = newProcs{i};

        % Since we aren't storing movie objects in the userData struct, we
        % don't need to update the userData MOs
        
        movie = proc.getOwner();
        userData.package(iMovie(i)) = movie.getPackage(movie.getPackageIndex(userData.packageName,1,false)); % we need to know which package to get!
        
        % Update crtPackage if it is the one currently selected
        if(userData.id == iMovie)
            userData.crtPackage = userData.package(iMovie(i));
        end
    end
end

% --------------------------------------------------------------------
function menu_parallel_cluster_profile_Callback(cluster_profile)
% hObject    handle to menu_parallel_cluster_profile (see GCBO)
% eventdata  reserved - to be defined in a future version of MATLAB
% handles    structure with handles and user data (see GUIDATA)
if(strcmp(hObject.Label,'None'))
    cluster = [];
else
    cluster = parcluster(cluster_profile);
    poolSizeMenu = handles.menu_parallel_pool;
    delete(poolSizeMenu.Children);
    currentPool = gcp('nocreate');
    h = uimenu(poolSizeMenu,'Label','No Pool','Callback',{@menu_parallel_pool_size_Callback,handles});
    % Provide at most 16 options
    N = max(floor(cluster.NumWorkers/16),1);
    if(isempty(currentPool))
        if(strcmp(cluster.Profile,'local'))
            % For local profile, set the default to maximum number of
            % workers
            poolSize = cluster.NumWorkers;
        else
            % For other profiles, use the first increment
            poolSize = N;
        end
    else
        % If pool exists, display current size
        poolSize = currentPool.NumWorkers;
    end

    % List pool sizes for every N workers
    % make sure existing pool size is checked
    poolSizeCheckExists = false;
    for i=N:N:cluster.NumWorkers
        h = uimenu(poolSizeMenu,'Label',num2str(i),'Callback',{@menu_parallel_pool_size_Callback,handles});
        if(i == N)
            h.Separator = 'on';
        end
        if(i == poolSize)
            h.Checked = 'on';
            poolSizeCheckExists = true;
        end
    end
    if(~poolSizeCheckExists)
        h = uimenu(poolSizeMenu,'Label',num2str(poolSize),'Callback',{@menu_parallel_pool_size_Callback,handles});
        h.Checked = 'on';
    end
end
% Set uTrackParCluster so that this information is accessible
uTrackParCluster(cluster);
batchMenuItem = findobj(handles.menu_parallel_batch,'Checked','on');
batchMenuItem.Callback(batchMenuItem, eventdata);
end