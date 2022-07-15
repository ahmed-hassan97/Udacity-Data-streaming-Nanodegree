CREATE TABLE accounts (
	id serial PRIMARY KEY,
	username VARCHAR ( 50 ) NOT NULL,
	pass VARCHAR ( 50 ) NOT NULL,
	created_on TIMESTAMP with time zone default current_timestamp NOT NULL,
    last_login TIMESTAMP with time zone default current_timestamp NOT NULL
);

insert into accounts (id , username , pass) values
(1 , "jdkfhrugyhh" , "35e67dyedfhx6hdhey6f"),
(2 , "ghggjnghg" , "dfdfdfdvfgrfdd"),
(3 , "ghtgjiuiy" , "dfxsfofhdfmcejdfn"),
(4 , "sdfgghffdf" , "dfklsnfudk"),
(5 , "djkfdm,fndj" , "dsdfdfgvcxvcsd"),
(6 , "fdufndfkmdk" , "qeerrteedxsd"),
(7 , "edfcesfvcdszdx" , "sdcsxfrerdcdgfc"),
(8 , "csfcdvdcs" , "azxccxvfdc sdx"),
(9 , "xcsfcesfcds" , "sdwsfefedfceerhe"),
(10 , "sfsdcsxc" , "zxscdgfeddqxsf"),
(11 , "sdxwsdcxwsd" , "ssxfvrrsxfwsdcdv"),
(12, "csdx" , "zceefdwscgvecs"),
(13, "csdx" , "zceefdwscgvecs"),
(14, "xsedwsc w" , "xsfgfcxsedwsdvc"),
(15, "xcwsdxsfc" , "sfvewdxwwqafer"),
(16, "sfcwscd" , "azdcdfdxzcs"),
(17, "dsfcxc" , "zcddeaxdgfdsz"),
(18, "dasswsdc" , "zcdxazcszdxz"),
(19, "sdcsfcvsa" , "cxzdxwedescxsaas"),
(20, "zcscda" , "swededvcxedws"),
(21, "fcssedcsdf" , "xfcewaszxvcdwadx"),
(22, "dwrfedsfcxa" , "zcxq2wedesdzxzae2w"),
(23, "zcwedxsxfvc" , "xzweaswsxvcxewd"),
(24, "xfwesdsxzfds" , "xewesqxazgfw"),
(25, "sfcwsdcdfd" , "xvcwedfeszx"),
(26, "sfgwdxcdsfd" , "zcedwsvcsaedcd")

