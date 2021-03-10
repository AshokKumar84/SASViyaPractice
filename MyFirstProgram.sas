/*****************************************************************************/
/*  Start a session named mySession using the existing CAS server connection */
/*  while allowing override of caslib, timeout (in seconds), and locale     */
/*  defaults.                                                                */
/*****************************************************************************/

cas mySession sessopts=(caslib=casuser timeout=1800 locale="en_US");

/*** Assigns all default CAS Library ***/
caslib _all_ assign;

/*** Loads the data from SASHELP library to CASUSER library ***/
/*** CASUSER library - This is a default library inside CAS Server 
similar to SASUSER library inside SAS Server ***/
proc casutil;
	load data = sashelp.cars casout = "mycars" promote;
run;

proc mdsummary data=casuser.mycars;
	var mpg_city;
	groupby type/out = casuser.carsinfo(promote=yes);
run;

/* proc casutil; */
/* 	load file = "&datadir./WorldData.xlsx" casout = "myworlddata"; */
/* 	contents casdata="myworlddata"; */
/* run; */

/*** Shows the contents of MyCars Dataset resides inside CASUSER library ***/
proc casutil;
	contents casdata = "MyCars";
run;

/*** List of tables in CASUSER library ***/
proc casutil;
	list;
run;

/*** Saving permenantly to CASUSER library ***/
proc casutil;
	save casdata="MyCars" replace;
run;

/*** Assign SAS library to CAS Server ***/
libname  mycas cas;

/*** List all Active CAS Sessions ***/
cas _all_ list;

/*** Terminate the Active SAS Session ***/
cas _all_ terminate;

/*** SAS 9.4 ***/
libname orion "E:\Work\SAS Viya\";

data cars;
	set sashelp.cars;
	Average_MPG=mean(MPG_City, MPG_Highway);
	Keep Make Model Type MSRP Average_MPG;
run;

cas mysess sessopts=(caslib=casuser);

libname mycas cas caslib=casuser;

/*** Loads cars dataset to In-Memory CASLIB (mycas) from Source Data space ***/
proc casutil;
	load data=sashelp.cars replace;
run;

/*** Modify In-Memory CASLIB (mycas) data ***/
data mycas.cars;
	set mycas.cars;
	
	Average_MPG=mean(MPG_City, MPG_Highway);
	Keep Make Model Type MSRP Average_MPG;
run;

/** SAVE - Save physical copy to Source Data space in the formof SASHDAT  ***/
/*** DELETE - Delete Physical copy from Source Data spacarce ***/
proc casutil;
	save casdata="cars" replace;
	droptable casdata="cars";
run;

proc casutil;
	list tables;
run;


data bigcars;
	set sashelp.cars;
	do i=1 to 100;
	output;
	end;
run;

data bigcars_score;
	set bigcars;
	length myscore 8;
	myscore=0.3*Invoice/(MSRP-Invoice) + 0.5*(EngineSize+Horsepower)/Weight + 0.2*(MPG_City+MPG_Highway);
run;

libname mycas cas caslib=casuser;

proc casutil;
	load data = sashelp.cars replace;
run;

data mycas.bigcars;
	set mycas.cars;
		do i=1 to 100;
			output;
		end;
run;

data mycas.bigcars_score;
	set mycas.bigcars;
	length myscore 8;
	myscore=0.3*Invoice/(MSRP-Invoice) + 0.5*(EngineSize+Horsepower)/Weight + 0.2*(MPG_City+MPG_Highway);
	Thread = _threadid_; 
run;


/*** SAS Viya don't need PROC SORT before the data step BY group ***/
/*** Groups the data based on first BY group and sort it based on subsequent variable in BY group ***/
data mycas.cars2;
	set mycas.cars;
	Average_MPG=mean(MPG_City, MPG_Highway);
	keep Make Model Type Average_MPG MSRP LowMSRP HighMSRP;
	by Type MSRP;
	
	if first.Type then LowMSRP=1;
		else LowMSRP=0;
	if last.Type then HighMSRP=1;
		else HighMSRP=0;
run;


/*** Partition - variables to divide and distribute the rows of the input table across threads. ***/
/*** OrderBy - Orders the row within each Partition ***/
data mycas.cars2 (partition=(Type) orderby=(MSRP));
	set mycas.cars;
	Average_MPG=mean(MPG_City, MPG_Highway);
	keep Make Model Type Average_MPG MSRP LowMSRP HighMSRP;
	by Type;

	if first.Type then LowMSRP=1;
		else LowMSRP=0;
	if last.Type then HighMSRP=1;
		else HighMSRP=0;
run;

/*** Program to run in SAS9 - SAS workspace server ***/
proc format;
	value pricerange_sas low-25000=”Low”
	25000<-50000=”Mid”
	50000<-75000=”High”
	75000<-high=”Luxury”;
run;

data cars_formatted;
	set sashelp.cars;
	format MSRP pricerange_sas.;
	keep Make Model MSRP MPG_Highway;
run;

proc print data=cars_formatted;
run;


/*** Program to run in SAS Viya - CAS Server ***/
cas mysession sessopts=(caslib=casuser);
libname mycas cas;

proc format casfmtlib='casformats';
	value pricerange_cas low-25000=”Low”
	25000<-50000=”Mid”
	50000<-75000=”High”
	75000<-high=”Luxury”;
run;

data mycas.cars_formatted; 
	set sashelp.cars;
	format MSRP pricerange_cas.;
	keep Make Model MSRP MPG_Highway;
run;

proc mdsummary data=mycas.cars_formatted;
	var MPG_Highway;
	groupby MSRP / out=mycas.cars_summary;
run;


PROC CASUTIL;
	LOAD DATA=sashelp.cars OUTCASLIB="demoCas"
	CASOUT="demoTable" replace;
RUN;
