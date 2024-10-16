function newchannel = change_channel_path(c,path,owner)
arguments
    c (1,1) Channel
    path (1,1) string
    owner {mustBeNonempty} = false
end


channel_info = unpack(c);
if exist('owner','var') && (owner ~= false)
     % third parameter does not exist, so default it to something
     owner_array = {'owner_';owner};
     channel_info = [channel_info; owner_array];
else
     channel_info = remove_kwarg(channel_info,'owner_');
end

%disp(channel_info)

%idx = find(strcmp([channel_info{1:2:end}], 'channelPath_')); %find the 'channelPath_' property in the properties kwargs
%disp(idx)

%channel_info(idx:idx+1) = [];

%disp(channel_info)

newchannel = Channel(char(GetFullPath(path)),channel_info{:});