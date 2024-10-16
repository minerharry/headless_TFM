function trimmed = remove_empty_kwargs(kwargs)
arguments
    kwargs (:,:) cell
end
idxs = find(cellfun(@isempty,kwargs(2:2:end)));
trimmed = kwargs;
for i=length(idxs):-1:1
    idx = idxs(i)*2;
    trimmed(idx) = [];
    trimmed(idx-1) = [];
end