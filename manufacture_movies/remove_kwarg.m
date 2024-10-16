function removed  = remove_kwarg(kwargs,keys)
% Takes in a cell of keyword arguments {'key',value,'key',value} and a key
% removes that key and its value from the list
arguments
    kwargs (:,:) cell
end
arguments (Repeating)
    keys (1,1) string
end
for i=1:length(keys)
    key = keys(i);
    key = key{1};
    idx = 2*find(strcmp(kwargs(1:2:end), key))-1; %find the 'channelPath_' property in the properties kwargs
    %disp(idx)
    kwargs(idx:idx+1) = [];
end
removed = kwargs;

