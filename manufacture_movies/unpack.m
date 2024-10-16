function c1 = unpack(s)
arguments
    s (1,1) struct
end
fieldn = fieldnames(s);
fieldv = struct2cell(s);
nfield = length(fieldn);
c1 = cell(2*nfield,1);
c1(1:2:end) = fieldn;
c1(2:2:end) = fieldv;
end