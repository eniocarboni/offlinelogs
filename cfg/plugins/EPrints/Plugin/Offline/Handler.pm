=pod

=head1 NAME

B<EPrints::Plugin::Offline::Handler> - EPrints offline/online access handler

=head1 DESCRIPTION

This module should put offline the many GB of log table access of the software eprints
 See http://www.eprints.org/
 The minimum amount that can be taken offline is the month.
 When a month is in offline mode can be put online or permanently deleted

Attention: in EPrints version before or equal 3.3.7 be sure to patch EPrints::Database as in commit
 https://github.com/eprints/eprints/commit/20a73ce081a4cad002cfa28b080ef09767d644ca

I<checkBugs()> method can help you find if the version of EPrints needs to be patched

Offline access dir default in config path B<variables_path> plus B</offline> or what else set in B<z_offline.pl>. 
 This directory must be manual created before use this module.
 In this directory will be saved each file for every table references access dataset and for every months of log plus an index file named access.index.

For any changes the access.index will be first rotated and then changed. 
 The last 20 rotated index will be stored as .access.index.<n> where <n> from 1 to 20. 

This max value (20) can be modified as option of new method o setting B<max_rotate_index> in config file B<z_offline.pl>. 
B<max_rotate_index> must be >0 or default to 20.

Each log file will be named YYYYMM_<table_name>.csv.gz where YYYYMM is the year and the month of the log and <table_name> is the table's name in offline state.
 This files are in gzip format and are .csv (comma separated file) where the separated field is <TAB>.
 The first line are comment line (starting with #) with each field is the name of the field of the table put offline.

The index file is a simple csv file where the separated field is <TAB> and the fields are:

	1) YYYYMM
	2) TOT (number of record in offline state)
	3) DOWNLOADS (downloads in YYYYMM)
	4) VIEWS (views in YYYYMM)
	5) ID_MIN (min id of dataset for YYYYMM)
	6) ID_MAX (max id of dataset for YYYYMM)

=head1 CONFIG FILE 

B<ARCHIVEID/cfg/cfg.d/z_offline.pl>

$c->{offline}= 
  {
        # logdir: directory (must exist) where we save the access log in offline state
        #  if empty we use $repo->get_conf('variables_path')."/access_offline"
        logdir          =>'',
        # months_to_keep: are the months to keep online in access table
        months_to_keep  =>1,
        # Max number of rotation for the index file before any update (def. 20)
        max_rotate_index=>20,
	# use optimize table if DBD module is mysql and Engine is MyISAM or InnoDB
        #  after many sql delete commands
        # For InnoDB engine check the sql command "show global variables like 'innodb_file_per_table'"
        #  to see that innodb_file_per_table=ON that is tablespace is for single table
        # (def. 1)
        optimize=>1,
        # optimize table if Data_free > (Data_length + Index_length) * optimize_if_data_free_perc / 100
        #  use 'show table status like <table_name>' to find data
        #  def. 20
        optimize_if_data_free_perc=>20,
	# history if set operation will be saved in dataset history otherwise nothing wil be saved in history (def. 1)
        history=>1,
  };

=head1 SYNOPSIS

	use EPrints;
	my ($session,$repoid,$verbose,$test,$handler);
	$repoid='my_repoid';
	my $session = new EPrints::Session( 1 , $repoid );
	if( !defined $session )
	{
	        print STDERR "Failed to load repository: $repoid\n";
	        exit 1;
	}
	$verbose=1; $test=0;
	my $handler = $session->plugin( "Offline::Handler", verbose => $verbose, test=>$test );
	unless($handler)
	{
	        print STDERR "FATAL ERROR: Offline handler (Offline::Handler.pm) not available\n";
	        $session->terminate();
	        exit;
	}
	
	if ($handler->checkBugs() ) {
	        $session->terminate;
	        exit;
	}
	# I enable test mode for caution
	$handler->test(1); 
	$handler->offlineProcess();

=head1 METHODS

=cut

package EPrints::Plugin::Offline::Handler;

use Time::HiRes qw(gettimeofday tv_interval);
use IO::Compress::Gzip;
use IO::Uncompress::Gunzip;

our @ISA = qw/ EPrints::Plugin /;
our $VERSION = '0.1';

use strict;

=pod

=over 

=item my $handler = $session->plugin( "Offline::Handler", %opts )

Creates and returns a new plugin EPrints::Plugin::Offline::Handler object

%opts:

	datasetid (def. 'access', not tested else)
	verbose (def. 0)
	date (fixed date YYYYMM)
	todate (since date YYYYMM)
	test (def.0, if 1 don't delete record and don't write index file)
	max_rotate_index (def. 20)
	optimize (def. 1)
	optimize_if_data_free_perc (def. 20)
	history (def. 1)

=cut

# Offline::Handler
#
# The main method
sub new
{
	my( $class, %params ) = @_;
	my $self = $class->SUPER::new( %params );
	my ($offline);
	$self->{verbose} ||= 0;
	if( !defined $self->{session} )
	{
		$self->log("no session present ... exit now",0);
		return undef;
	}
	$offline=$self->{session}->get_repository->get_conf("offline");
	if (! $offline || ref($offline) ne 'HASH') {
		$offline={};
	}
	if ( ! exists $offline->{'logdir'} || ! $offline->{'logdir'}) {
		$self->{logs_dir}=$self->{session}->get_repository->get_conf("variables_path")."/offline";
	}
	else { 	$self->{logs_dir}=$offline->{'logdir'}; }
	
	$self->{dbh} = $self->{session}->get_database;
	
	$self->{datasetid}||='access';
	
	# Max number of rotation for the index file before any update (def. 20)
	$self->{max_rotate_index}='' unless defined $self->{max_rotate_index};
	$self->{max_rotate_index}=~s/^0+//;
	if ( $self->{max_rotate_index} !~/\d+$/ ) {
		$self->{max_rotate_index}=$offline->{max_rotate_index};
		$self->{max_rotate_index}=~s/^0+//;
		$self->{max_rotate_index}=$self->{max_rotate_index}=~/\d+$/ ? $self->{max_rotate_index} : 20;
	}

	# use optimize table if DBD module is mysql and Engine is MyISAM or InnoDB
	#  after many sql delete commands
	# For InnoDB engine check the sql command "show global variables like 'innodb_file_per_table'"
	#  to see that innodb_file_per_table=ON that is tablespace is for single table
	# (def. 1)
	$self->{optimize}='' unless defined $self->{optimize};
	if ( $self->{optimize} !~/^(0|1)$/ ) {
		$self->{optimize}=$offline->{optimize};
		$self->{optimize}=1 unless defined $self->{optimize};
	}
	
	# optimize table if Data_free > (Data_length + Index_length) * optimize_if_data_free_perc / 100
	#  use 'show table status like <table_name>' to find data
	#  def. 20
	$self->{optimize_if_data_free_perc}='' unless defined $self->{optimize_if_data_free_perc};
	$self->{optimize_if_data_free_perc}=~s/^0+//;
	if ( $self->{optimize_if_data_free_perc} !~/\d+$/ ) {
		$self->{optimize_if_data_free_perc}=$offline->{optimize_if_data_free_perc};
		$self->{optimize_if_data_free_perc}=~s/^0+//;
		$self->{optimize_if_data_free_perc}=$self->{optimize_if_data_free_perc}=~/\d+$/ ? $self->{optimize_if_data_free_perc} : 20;
	}

	# history (def. 1)
	$self->{'history'}='' unless defined $self->{'history'};
	if ( $self->{history} !~/^(0|1)$/ ) {
		$self->{history}=$offline->{history};
		$self->{history}=1 unless defined $self->{history};
	}
	return $self;
}


=pod

=item I<checkbugs()>

check for very important EPrints bug not patched and return 1 if found

At this time check for EPrints version <=3.3.7 for commit https://github.com/eprints/eprints/commit/20a73ce081a4cad002cfa28b080ef09767d644ca

=cut

# check bug in EPrints::Database.pm on method prepare_select()
#  only for Engine mysql e Pg and EPrints version <=3.3.7
# bug: swap limit with offset
# see commit 20a73ce081a4cad002cfa28b080ef09767d644ca in
# https://github.com/eprints/eprints/commit/20a73ce081a4cad002cfa28b080ef09767d644ca
sub checkBugs {
	my $self=shift;
	my ($db,$sth,$cmd,$version,@vers);
	$db=$self->{dbh};
	$version=$db->get_version;
	@vers=$version=~/(\d+)/g;
	if ($vers[0]<3 || ($vers[0]=3 && $vers[1]<3) || ( $vers[0]=3 && $vers[1]=3 && $vers[2]<=7) ) {
		if (ref($db)=~/::(mysql|Pg)$/) {
			$cmd="SELECT eprintid FROM eprint";
			$sth=$db->prepare_select($cmd,limit=>1,offset=>0);
			unless ($db->{dbh}->{Statement}=~/ LIMIT 1 OFFSET 0/) {
				$self->log( "Find bugs in EPrints::Database.pm on method prepare_select(). See commit https://github.com/eprints/eprints/commit/20a73ce081a4cad002cfa28b080ef09767d644ca",0 );
				$self->log( "This bug is before EPrints version 3.3.7 and your is $version",0 );
				$self->log( "Apply the patch and play again, please",0 );
				return 1;
			}
		}
	}
	return 0;
}

=item I<debug()>

Set or get verbose output to STDERR

=cut

# Turns on/off the debugging.
sub debug
{
        my( $self, $debug ) = @_;
	if ($debug && $debug !~/^\d+$/) { $debug=1; }
        $self->{verbose} = $debug;
}

=item I<test()>

Set or get the state of test mode

=cut

sub test {
	my $self=shift;
	my $test=shift;
	if ($test) {
		$self->{test}=1;
	}
	return $self->{test};
}

=item I<history()>

Set or get the state of history mode

In history mode nothing will be saved in dataset history 

=cut

sub history {
	my $self=shift;
	my $history=shift;
	if ($history) {
		$self->{history}=1;
	}
	elsif (defined $history) {
		$self->{history}=0;
	}
	return $self->{history};
}

=item I<log($msg,[$pri])>

log to STDERR the $msg if $pri > $self->debug(). 
If $pri is false log however

=cut 

# Write into the Apache logs if $pri <= $self->{verbose}
sub log
{
        my( $self, $msg, $pri ) = @_;
	$pri=1 unless defined $pri;
        return if( $pri > $self->{verbose} );
        $self->{session}->log( $msg );
}

=item I<checkLogDir()>

Return true if logs_dir is valid and exist

=cut

sub checkLogDir {
	my $self=shift;
	if (! $self->{logs_dir} || ! -d $self->{logs_dir}) {
		$self->log("La directory '".$self->{logs_dir}."' non esiste. Bisogna crearla. ".
			"Per cambiare il path configurare \$c->{'offline'} impostando la variabile 'logdir'.",0);
		return undef;
	}
	return 1;
}

=item I<logFileName($yyyymm,[$suffisso,$ext])>

Return the file name to offline $yyyymm,[$suffisso,$ext]

=cut

sub logFileName 
{
	my $self=shift;
	my $yyyymm=shift;
	my $suffisso=shift || '';
	my $ext=shift || 'csv.gz';
	my $logs_dir=$self->getLogDir;
	return sprintf('%s/%06d_%s.%s',$logs_dir,$yyyymm,$suffisso,$ext);
}

=item I<removeStaleFile($yyyymm)>

unlink stale offline file that not match on index file

=cut

sub removeStaleFile 
{
	my $self=shift;
	my $yyyymm=shift;
	my ($file_glob,@file_stales);
	my $logs_dir=$self->getLogDir;
	$file_glob=sprintf('%s/%06d_*.csv.gz',$logs_dir,$yyyymm);
	@file_stales=glob $file_glob;
	foreach (@file_stales) {
		if (unlink $_) {
			$self->log( "  delete stale file $_");
		}
	}
}

=item I<normalizeCsv($el)>

Normalizza $el for csv field

=cut

sub normalizeCsv 
{
	my $self=shift;
	my $el=shift;
	foreach (@$el) {
		$_='' unless defined $_; 
		$_=~s/\n/\\n/g;
		$_=~s/\r/\\r/g;
		$_=~s/\t/\\t/g;
	}
}

=item I<findMinIdToProcess()>

Find and return the first (min) id to process

=cut

sub findMinIdToProcess 
{
	my $self=shift;
	my ($db,$sth,$cmd,$index,@keys_index,$idx,$table,$id,$dy,$dm,$yyyy,$mm);
	my $session=$self->{session};
	$db=$self->{dbh};
	$index=$self->readIndex();
	# incremental work
	@keys_index=reverse(sort keys %$index);
	$idx=$keys_index[0] && exists $index->{$keys_index[0]} ? $index->{$keys_index[0]}{id_max} : 0;
	$idx||=0;
	# fixed date (una tantum): not incremental work
	if ( $self->{date} ) { 
		$idx=exists $index->{ $self->{date} }{id_max} ? $index->{ $self->{date} }{id_max} : 0;
		$table=$self->getPrimaryTable();
		$table=$db->quote_identifier($table);
		$id=$self->getPrimaryIndexTables();
		$id=$db->quote_identifier($id);
		$dy=$db->quote_identifier('datestamp_year');
		$dm=$db->quote_identifier('datestamp_month');
		($yyyy,$mm)=$self->{date}=~/^(\d{4})(\d{2})/;
		$cmd=qq|SELECT $id FROM $table WHERE $id>? and ($dy=? AND $dm=?) ORDER BY $id|;
		$sth=$db->prepare_select($cmd,limit=>1);
		$sth->bind_param(1,$idx);
		$sth->bind_param(2,$yyyy);
		$sth->bind_param(3,$mm);
		$db->execute($sth,$db->{dbh}->{Statement}." [$idx,$yyyy,$mm]");
		($idx)=$sth->fetchrow_array;
		$sth->finish;
	}
	return $idx;
}

=item I<findMaxIdToNotProcess()>

Find and return the last (max) id to process

=cut

sub findMaxIdToNotProcess 
{
	my $self=shift;
	my ($db,$sth,$cmd,$idx,$table,$id,$dy,$dm,$yyyy,$mm);
	my $session=$self->{session};
	$db=$self->{dbh};
	$table=$self->getPrimaryTable();
	$table=$db->quote_identifier($table);
	$id=$self->getPrimaryIndexTables();
	$id=$db->quote_identifier($id);
	$dy=$db->quote_identifier('datestamp_year');
	$dm=$db->quote_identifier('datestamp_month');
	if ($self->{todate} && $self->{todate}=~/^\d\d\d\d\d\d$/) {
		($yyyy,$mm)=$self->{todate}=~/^(\d{4})(\d{2})/;
	}
	elsif ($self->{date} && $self->{date}=~/^\d\d\d\d\d\d$/) {
		($yyyy,$mm)=$self->{date}=~/^(\d{4})(\d{2})/;
	}
	else {
		($yyyy,$mm)=$self->ConvertFromMonthstokeepToYYYYMM();
	}
	$self->{todate}=join('',$yyyy,$mm);
	$cmd=qq|SELECT $id FROM $table WHERE ($dy=? AND $dm>?) OR $dy>? ORDER BY $id|;
	$sth=$db->prepare_select($cmd,limit=>1);
	$sth->bind_param(1,$yyyy);
	$sth->bind_param(2,$mm);
	$sth->bind_param(3,$yyyy);
	$db->execute($sth,$db->{dbh}->{Statement}." [$yyyy,$mm,$yyyy]");
	($idx)=$sth->fetchrow_array;
	$sth->finish;
	return $idx;
}

=item I<getPrimaryIndexTables()>

Return the primary index of dataset $self->{datasetid}

=cut

sub getPrimaryIndexTables 
{
	my $self=shift;
	my $session=$self->{session};
	my $ds = $session->get_repository->get_dataset( $self->{datasetid} );
	my $idx=$ds->key_field->get_name;
	$self->log("getPrimaryIndexTables=$idx",2);
	return $idx;

}

=item I<getPrimaryTable()>

Return the primary table of dataset $self->{datasetid}

=cut

sub getPrimaryTable 
{
	my $self=shift;
	my $session=$self->{session};
	if (exists $self->{__getPrimaryTable__} ) {
		return $self->{__getPrimaryTable__};
	}
	my $ds = $session->get_repository->get_dataset( $self->{datasetid} );
	$self->{__getPrimaryTable__}=$ds->get_sql_table_name;
	$self->log("getPrimaryTable=".$self->{__getPrimaryTable__},2);
	return $self->{__getPrimaryTable__};
}

=item I<getSecondaryTables()>

Return the secondary tables of dataset $self->{datasetid} as array ref

=cut

sub getSecondaryTables 
{
	my $self=shift;
	my ($languages,$ds,@fields,$db,@tables,$t,$lang);
	my $session=$self->{session};
	if (exists $self->{__getSecondaryTables__} && ref $self->{__getSecondaryTables__} eq 'ARRAY') {
		return $self->{__getSecondaryTables__};
	}
	$languages=$session->get_repository->get_conf("languages");
	$ds = $session->get_repository->get_dataset( $self->{datasetid} );
	@fields=$ds->fields;
	$db=$session->get_db;
	@tables=();
	$t=$ds->get_sql_index_table_name(); # index table
	push @tables,$t if $db->has_table($t);
	$t=$ds->get_sql_grep_table_name();  # LIKE table
	push @tables,$t if $db->has_table($t);
	$t=$ds->get_sql_rindex_table_name(); # reverse text table
	push @tables,$t if $db->has_table($t);
	foreach $lang (@$languages) {
		$t=$ds->get_ordervalues_table_name($lang); # ordervalues table ($lang)
		push @tables,$t if $db->has_table($t);
	}
	foreach (@fields) {
		if ($_->property('multiple') ) {
			$t=$ds->get_sql_sub_table_name($_); # multi field store in separate table
			push @tables,$t if $db->has_table($t);
		}
	}
	$self->log("getSecondaryTables=".join(", ",@tables),2 );
	$self->{__getSecondaryTables__}=\@tables;
	return \@tables;
}

=item I<rotateIndex()>

Rotate the index file (max 20). if $self->{test} nothing done

=cut

sub rotateIndex 
{
	my $self=shift;
	my ($logs_dir,$i,$ii,$table,$rotate);
	# return if in test mode
	return if $self->{test};
	my $session=$self->{session};
	$logs_dir=$self->getLogDir();
	$table=$self->getPrimaryTable();
	$rotate=$self->{max_rotate_index};
	return unless ( -e "$logs_dir/$table.index" );
	if (-e "$logs_dir/.$table.index.$rotate") {
		unlink "$logs_dir/.$table.index.$rotate";
	}
	$rotate--;
	for $i (reverse(1..$rotate)) {
		if (-e "$logs_dir/.$table.index.$i") {
			$ii=$i+1;
			rename "$logs_dir/.$table.index.$i","$logs_dir/.$table.index.$ii";
		}
	}
	rename "$logs_dir/$table.index","$logs_dir/.$table.index.1";
	$self->log("  rotated $table index file");
}

=item I<readIndex([$force])>

Read from file the index and return a reference to it.

The fisical read is cached in $self->{__readIndex__} and only if $force we re-read on file

=cut

sub readIndex 
{
	my $self=shift;
	my $force=shift;
	my ($logs_dir,$r,@el,$index,$table);
	if (! $force && exists $self->{__readIndex__} && ref($self->{__readIndex__}) eq 'HASH' ) {
		return $self->{__readIndex__};
	}
	my $session=$self->{session};
	$logs_dir=$self->getLogDir();
	$table=$self->getPrimaryTable();
	$index={};
	return $index unless ( -e "$logs_dir/$table.index" );
	open (INDEX, "$logs_dir/$table.index" ) or die $!;
	# '#YYYYMM','TOT','DOWNLOADS','VIEWS','ID_MIN','ID_MAX'
	while ($r=<INDEX>) {
		chomp $r;
		next if $r=~/^\s*#/; # comment line
		@el=split("\t",$r);
		if (scalar(@el)<6) {
			$self->log("  skipping bad index line ($r)",0);
			next;
		}
		$index->{$el[0]}{tot}=$el[1];
		$index->{$el[0]}{download}=$el[2];
		$index->{$el[0]}{view}=$el[3];
		$index->{$el[0]}{id_min}=$el[4];
		$index->{$el[0]}{id_max}=$el[5];
	}
	close(INDEX);
	$self->log("read index file");
	$self->{__readIndex__}=$index;
	return $self->{__readIndex__};
}

=item I<writeIndex($index)>

Write index file as value in ref $index.
Before write the access file is rotated by I<rotateIndex>

=cut

sub writeIndex 
{
	my $self=shift;
	my $index=shift;
	my ($logs_dir,$yyyymm,$table);
	return if ref($index) ne 'HASH';
	# return if in test mode
	return if $self->{test};
	my $session=$self->{session};
	$logs_dir=$self->getLogDir();
	$table=$self->getPrimaryTable();
	$self->rotateIndex();
	open (INDEX, ">$logs_dir/$table.index" ) or die $!;
	print INDEX join("\t",'#YYYYMM','TOT','DOWNLOADS','VIEWS','ID_MIN','ID_MAX')."\n";
	foreach $yyyymm (sort keys %$index) {
		print INDEX join("\t",$yyyymm,$index->{$yyyymm}{tot},$index->{$yyyymm}{download},
			$index->{$yyyymm}{view},$index->{$yyyymm}{id_min},$index->{$yyyymm}{id_max})."\n";
	}
	close INDEX;
	$self->log("  write new $table index");
}

=item I<getLogDir()>

Return the Log dir where save the offline logs

=cut

sub getLogDir 
{
	my $self=shift;
	return $self->{logs_dir};
}

=item I<ConvertFromMonthstokeepToYYYYMM()>

Translate B<months_to_keep> in B<offline> config params as YYYYMM

=cut

sub ConvertFromMonthstokeepToYYYYMM 
{
	my $self=shift;
	my ($months_to_keep,@ltime);
	$months_to_keep=$self->{session}->get_repository->get_conf("offline",'months_to_keep');
	unless ($months_to_keep=~/^\d+$/) {
		$months_to_keep||=6; # 6 mesi
	}
	@ltime=localtime(time - ($months_to_keep*30*24*60*60));
	$ltime[4]++;
	$ltime[5]+= 1900;
	return ($ltime[5],$ltime[4]);
}

=item I<optimizeProcess()>

Optimize all tables if necessary

=cut

sub optimizeProcess {
	my $self=shift;
	my ($db,$sth,$cmd,$acc,$tables,$t,$ref);
	$db=$self->{dbh};
	unless (ref($db)=~/::mysql$/) {
		$self->log("optimize table works only on mysql database",0);
		return;
	}
	$self->log("Start optimize process",0);
	$acc=$self->getPrimaryTable();
	$tables=$self->getSecondaryTables();
	foreach $t ( ($acc,@$tables) ) {
		$cmd='show table status like '.$db->quote_value($t);
		$sth=$db->prepare($cmd);
		$db->execute($sth,$cmd);
		$ref = $sth->fetchrow_hashref();
		$sth->finish;
		if ($ref->{'Name'} && $ref->{'Name'} eq $t) {
			if ($ref->{'Engine'} =~/^MyISAM|InnoDB$/) {
				if ($ref->{'Data_free'} > ($ref->{'Data_length'}+$ref->{'Index_length'}) * $self->{'optimize_if_data_free_perc'} / 100) {
					$self->log(sprintf("  table %s need optimize ( Data_length=%d, Index_length=%d, Data_free=%d )",
						$t,$ref->{'Data_length'},$ref->{'Index_length'},$ref->{'Data_free'} ),0 );
					unless ($self->{optimize}) {
						$self->log("  optimize table $t disabled in config file ... skipping",1);
						next;
					}
					$self->log("  Start optimize table $t ...");
					$cmd="OPTIMIZE TABLE ".$db->quote_identifier($t);
					$sth=$db->prepare($cmd);
					$db->execute($sth,$cmd);
					$sth->finish;
					$self->log("  end optimize table $t");
					$cmd='show table status like '.$db->quote_value($t);
					$sth=$db->prepare($cmd);
					$db->execute($sth,$cmd);
					$ref = $sth->fetchrow_hashref();
					$sth->finish;
					$self->log(sprintf("  table %s after optimize: Data_length=%d, Index_length=%d, Data_free=%d",
						$t,$ref->{'Data_length'},$ref->{'Index_length'},$ref->{'Data_free'} ),0 );
				}
				else {
					$self->log(sprintf("  table %s NOT need optimize ( Data_length=%d, Index_length=%d, Data_free=%d )",
						$t,$ref->{'Data_length'},$ref->{'Index_length'},$ref->{'Data_free'} ),1 );
				}
			}
			else {
				$self->log("  table $t engine=".$ref->{'Engine'}.". Unmanaged ... skipping optimize table",1);
			}
		}
		else {
			$self->log("  Ops!!! command '$cmd' gave me back strange values or the table $t does not exist",1);
		}
	}
	$self->log("End optimize process",0);
}

=item I<saveHistory($details)>

Save in dataset history what is made offline/online/delete unless history is false in %opts of method new 
or if set false in history method.

B<$details> il the message set in field details of dataset history

=cut

sub saveHistory {
	my $self=shift;
	my $details=shift;
	return unless $self->{history};
	my $session=$self->{session};
	my $history_ds = $session->get_repository->get_dataset( "history" );
	$history_ds->create_object(
		$session,
		{
			#_parent=>$self,
			#userid=>$userid,
			datasetid=>$self->{datasetid},
			#objectid=>$self->get_id,
			#revision=>$self->get_value( "rev_number" ),
			actor =>"$0 @ARGV", # if old eprint's version before commit 907755e
			action=>'other',
			details=>$details
		}
	);
	$self->log("saved on ds history with action='other', details='$details'");
}

=item I<offlineProcessDeleteRecords($min,$max)>

Delete records on db on all tables of $self->{datasetid} from id B<$min> to id B<$max> (not included)

If in test mode ($self->{test}) don't delete records.

=cut 

sub offlineProcessDeleteRecords 
{
	my $self=shift;
	my $min=shift;
	my $max=shift;
	my ($db,$sth,$cmd,$acc,$tables,$t,$id,$idq,$t0,$n);
	return unless $max=~/^\d+$/;
	my $session=$self->{session};
	$db=$self->{dbh};
	$id=$self->getPrimaryIndexTables();
	$idq=$db->quote_identifier($id);
	$acc=$self->getPrimaryTable();
	$tables=$self->getSecondaryTables();
	foreach $t (@$tables,$acc) {
		$cmd=qq|DELETE FROM |.$db->quote_identifier($t).qq| WHERE $idq>=|.$db->quote_int($min).qq| AND $idq<|.$db->quote_int($max);
		if  (! $self->{test} ) {
			$t0 = [gettimeofday];
			$sth=$db->prepare($cmd);
			$db->execute($sth,$cmd);
			$n=$sth->rows();
			$t0=tv_interval ($t0);
			$self->log("  Delete $n records in $t0 seconds from table $t");
		}
		else {
			$self->log("  Test mode: deleting records from table $t");
		}
	}
}

=item I<offlineProcessOtherTables($yyyymm,$min,$max)>

Make offline process for all table of $self->{datasetid} except the main from id B<$min> to id B<$max> (not included) of month B<$yyyymm>

=cut

sub offlineProcessOtherTables 
{
	my $self=shift;
	my $yyyymm=shift;
	my $min=shift;
	my $max=shift;
	my ($db,$sth,$cmd,$filelog,$tables,$t,$ac_lang,$z,$start,$incr,$num,$empty,$idx,$id,$row);
	return unless $max=~/^\d+$/;
	my $session=$self->{session};
	$db=$self->{dbh};
	$id=$self->getPrimaryIndexTables();
	$id=$db->quote_identifier($id);
	$tables=$self->getSecondaryTables();
	foreach $t (@$tables) {
		$filelog=$self->logFileName($yyyymm,$t);
		if ( ! -e $filelog) { $empty=1; $row=0; } else { $empty=0; $row=999; }
		$z = new IO::Compress::Gzip("$filelog", Append => 1) or die "IO::Compress::Gzip failed: $IO::Compress::Gzip::GzipError\n";
		$start=0; $incr=1000; $num=1000;
		$cmd=qq|SELECT * FROM |.$db->quote_identifier($t).qq| WHERE $id>=? AND $id<?|;
		$sth=$db->prepare_select($cmd,limit=>1000,offset=>0);
		while ($num==1000) {
			$min=$idx+1 if $idx;
			$sth->bind_param(1,$min);
			$sth->bind_param(2,$max);
			$db->execute($sth,$db->{dbh}->{Statement}." [$min,$max]");
			$num=$sth->rows;
			if ( $empty ) {
				$empty=0;
				my $names = $sth->{'NAME'};
				print $z '#'.join("\t",@{$names})."\n";
			}
			while ($ac_lang=$sth->fetchrow_arrayref) {
				$idx=$ac_lang->[0];
				$self->normalizeCsv($ac_lang);
				print $z join("\t",@$ac_lang)."\n";
				$row++;
			}
		}
		close $z;
		# if empty unlink it (no row exists)
		unless ( $row ) { unlink $filelog; }
	}
}

=item I<offlineProcess()>

Make offline process for main table of $self->{datasetid}

This method save in history dataset in the details fields the value "offline accessid where datastamp in B<$date> and accessid from B<min> to B<max>"

=cut

sub offlineProcess 
{
	my $self=shift;
	my ($table,$start,$last,$index,$z,$t0,$cmd,$sth,$db,$ac,$min,$idx,
		$filelog,$yyyymm,$yyyymm_old,@ltime,
		$num,$empty,$cmd_last,$id,$idq,$discard,$new_to_offline,$log_tail);

	my $session=$self->{session};
	$db=$self->{dbh};
	return unless $self->checkLogDir();
	$index=$self->readIndex();
	$start=$self->findMinIdToProcess();
	unless (defined $start) {
		$self->log("Nothing to offline");
		return;
	}
	$last=$self->findMaxIdToNotProcess();
	$table=$self->getPrimaryTable();
	$id=$self->getPrimaryIndexTables();
	$idq=$db->quote_identifier($id);
	$self->log("Start offline process from $id=$start " .($last ? "to $id=$last" : '') );
	$idx=0;
	$filelog='';$num=1000; $min=1; $discard=0; $new_to_offline=0;
	# save on $self->{__OFFLINING__} the YYYYMM that is in working progress
	$self->{__OFFLINING__}='';
	$start--; 
	$cmd_last=$last ? " AND $idq<".$db->quote_int($last) : '';
	$cmd=qq|SELECT * FROM |.$db->quote_identifier($table).qq| WHERE $idq>? $cmd_last ORDER BY $idq|;
	$sth=$db->prepare_select($cmd,limit=>1000,offset=>0);
	while ($num==1000) {
		$start=$idx if $idx;
		$sth->bind_param(1,$start);
		$db->execute($sth,$db->{dbh}->{Statement} . " [$start]");
		$num=$sth->rows;
		while ( $ac=$sth->fetchrow_arrayref) {
			$idx=$ac->[0];
			# if year is NULL ?? !!!! or if month is NULL ?? !!!!
			if ( ! $ac->[1] || ! $ac->[2] ) {
				$discard++;
				next;
			}
			if ( $self->{date} && $self->{date} ne sprintf('%04d%02d',$ac->[1],$ac->[2]) ) {
				next;
			}
			$self->normalizeCsv($ac);
			$yyyymm=sprintf('%04d%02d',$ac->[1],$ac->[2]);
			if ($yyyymm ne $self->{__OFFLINING__}) {
				if ($self->{__OFFLINING__}) { # first time $self->{__OFFLINING__}=''
					close $z if $z;
					$self->offlineProcessOtherTables($self->{__OFFLINING__},$min,$idx);
					$t0=tv_interval ($t0);
					$log_tail=$new_to_offline != $index->{$self->{__OFFLINING__}}{'tot'} ? "; $new_to_offline new" : '';
					$log_tail.=$discard ? ", $discard discarded for NULL YYYYMM" : '';
					$self->log("  End offline ".$self->{__OFFLINING__}." in $t0 seconds (".$index->{$self->{__OFFLINING__}}{'tot'}." records$log_tail )");
					$self->writeIndex($index);
					$self->offlineProcessDeleteRecords($min,$idx);
					$self->saveHistory( "offline $id where datastamp in ".$self->{__OFFLINING__}." and $id from ".
						$index->{$self->{__OFFLINING__}}{id_min}." to ".$index->{$self->{__OFFLINING__}}{id_max} );
				}
				$discard=0;
				$new_to_offline=0;
				$min=$ac->[0];
				$self->{__OFFLINING__}=$yyyymm;
				# in case process by exacly date ($self->{date}), not in incremental process,
				#  discard this record if YYYYMM not equal $self->{date}
				# Example of event ($self->{date}="201301"):
				# accessid=n      ,datestamp like 201301
				# accessid=n+1    ,datestamp like 201301
				# ...
				# accessid=n+m    ,datestamp like 201301
				# accessid=n+m+1  ,datestamp like 201302
				# accessid=n+m+2  ,datestamp like 201302
				# accessid=n+m+z  ,datestamp like 201302
				# ...
				# accessid=n+m+z+1,datestamp like 201301
				# accessid=n+m+z+2,datestamp like 201301
				# ...

				if ( $self->{date} && $self->{date} ne sprintf('%04d%02d',$ac->[1],$ac->[2]) ) {
					$self->{__OFFLINING__}='';
					next;
				}
				$filelog=$self->logFileName($yyyymm,$table);
				$t0 = [gettimeofday];
				$self->removeStaleFile($yyyymm) unless exists $index->{$yyyymm};
				if (! -e $filelog) { $empty=1; } else { $empty=0;}
				$z = new IO::Compress::Gzip("$filelog", Append => 1) or die "IO::Compress::Gzip failed: $IO::Compress::Gzip::GzipError\n";
				if ( $empty ) {
					my $names = $sth->{'NAME'};
					print $z '#'.join("\t",@{$names})."\n";
				}
				$self->log("  Start offline $yyyymm");
				$index->{$yyyymm}{'id_min'}||=$min;
				$index->{$yyyymm}{'id_max'}||=$min;
				$index->{$yyyymm}{'tot'}||=0;
				$index->{$yyyymm}{'download'}||=0;
				$index->{$yyyymm}{'view'}||=0;
			}
			print $z join("\t",@$ac)."\n";
			$new_to_offline++; # Tot in this process
			$index->{$yyyymm}{'tot'}++; # Tot in this process +, if not null, others processed in other times
			$index->{$yyyymm}{'download'}++ if defined $ac->[12] && $ac->[12] eq '?fulltext=yes';
			$index->{$yyyymm}{'view'}++ if defined $ac->[12] && $ac->[12] eq '?abstract=yes';
			$index->{$yyyymm}{'id_max'}=$idx;
		}
	}
	if ($self->{__OFFLINING__}) {
		close $z if $z;
		$self->offlineProcessOtherTables($self->{__OFFLINING__},$min,$idx+1 );
		$t0=tv_interval ($t0);
		$log_tail=$new_to_offline != $index->{$self->{__OFFLINING__}}{'tot'} ? "; $new_to_offline new" : '';
		$log_tail.=$discard ? ", $discard discarded for NULL YYYYMM" : '';
		$self->log("  End offline ".$self->{__OFFLINING__}." in $t0 seconds (".$index->{$self->{__OFFLINING__}}{'tot'}." records$log_tail)");
		$self->writeIndex($index);
		$self->offlineProcessDeleteRecords($min,$idx+1);
		$self->saveHistory( "offline $id where datastamp in ".$self->{date}." and $id from ".
			$index->{$self->{__OFFLINING__}}{id_min}." to ".$index->{$self->{__OFFLINING__}}{id_max} );
		$self->optimizeProcess();
	}
	delete $self->{__OFFLINING__};
	$self->log("End offline process" );
}

=item I<onlineProcess()>

Make online process of month B<$date> or $self->{date}
 Online process revert the offline so all record saved on file will be stored in database and than unlink the offline file and update the index
 
This method save in history dataset in the details fields the value "online accessid where datastamp in B<$date> and accessid from B<min> to B<max>"

=cut

sub onlineProcess 
{
	my $self=shift;
	my ($db,$sth,$cmd,$pri_table,$tables,$t,$logs_dir,$index,$file,$z,$r,$placeholders,$i,$rp,$rec,$max,$min,$id);
	my $date=shift || $self->{date};
	$self->{date}=$date;
	my $session=$self->{session};
	$db=$self->{dbh};
	return unless $self->checkLogDir();
	$index=$self->readIndex();
	unless (exists $index->{$date}) {
		$self->log("date=$date is not in offline archive",0);
		return;
	}
	$self->log("Start online process" );
	$min=$index->{$date}{id_min};
	$max=$index->{$date}{id_max};
	# save on $self->{__ONLINING__} the YYYYMM that is in working progress
	$self->{__ONLINING__}=$date;
	$pri_table=$self->getPrimaryTable();
	$id=$self->getPrimaryIndexTables();
	$tables=$self->getSecondaryTables();
	$logs_dir=$self->getLogDir();
	foreach $t ( $pri_table,@$tables ) {
		$file="$logs_dir/$date"."_$t.csv.gz";
		unless (-e $file) {
			$self->log("Not find offline file $file ... skipping",1);
			next;
		}
		$self->log("working on $file",0);
		$z = new IO::Uncompress::Gunzip($file) or die "gunzip failed: $IO::Uncompress::Gunzip::GunzipError\n";
		$r=<$z>; # first row is comment for names rows
		$placeholders='';
		foreach (split(/\t/,$r)) {
			$placeholders.='?,';
		}
		$placeholders=~s/,$//; # drop last comma if exist
		$cmd=qq|INSERT INTO |.$db->quote_identifier($t).qq| VALUES ($placeholders);|;
		$sth=$db->prepare($cmd);
		$rec=0;
		while ($r=<$z>) {
			chomp $r;
			next if $r=~/^\s*#/; # drop comment line
			$i=0;
			foreach $rp (split(/\t/,$r)) {
				$i++;
				$rp=~s/\\n/\n/g;
				$rp=~s/\\r/\r/g;
				$rp=~s/\\t/\t/g;
				$sth->bind_param($i,$rp);
			}
			$r=~s/\t/","/g;
			$db->execute($sth,$cmd. qq| ["$r"]|);
			$self->log($cmd. qq| ["$r"]|,2);
			$rec++;
		}
		close $z;
		$self->log("   done on $file [insert $rec record on table $t]",0);
	}
	$self->saveHistory( "online $id where datastamp in $date and $id from $min to $max" );
	$self->deleteProcess();
	delete $self->{__ONLINING__};
	$self->log("End online process" );
}

=item I<deleteProcess()>

Make delete process of month B<$date> or $self->{date}
 Delete process permanently delete the offline log of the month B<$date> and than unlink the offline file and update the index
 
This method save in history dataset in the details fields the value "delete offline accessid where datastamp in B<$date> and accessid from B<min> to B<max>"

If in test mode, $self->test=true (see I<test()> method), nothing will be delete of saved in history dataset.

If this method is called by I<offlineProcess()> the history dataset will not be saved

=cut

sub deleteProcess 
{
	my $self=shift;
	my ($db,$sth,$cmd,$pri_table,$tables,$t,$logs_dir,$index,$file,$z,$r,$placeholders,$i,$rp,$rec,$max,$min,$id);
	my $date=shift || $self->{date};
	$self->{date}=$date;
	my $session=$self->{session};
	$db=$self->{dbh};
	return unless $self->checkLogDir();
	$index=$self->readIndex();
	unless (exists $index->{$date}) {
		$self->log("date=$date is not in offline archive",0);
		return;
	}
	# save on $self->{__DELETING__} the YYYYMM that is in working progress
	$self->{__DELETING__}='';
	$pri_table=$self->getPrimaryTable();
	$tables=$self->getSecondaryTables();
	$id=$self->getPrimaryIndexTables();
	$logs_dir=$self->getLogDir();
	foreach $t ( $pri_table,@$tables ) {
		$file="$logs_dir/$date"."_$t.csv.gz";
		unless (-e $file) {
			$self->log("Not find offline file $file ... skipping",0);
			next;
		}
		if (! $self->{test} ) {
			unlink "$file";
			$self->log("deleted offline log file $file",0);
		}
		else {
			$self->log("Test mode: deleted offline log file $file",0);
		}
	}
	if ( $self->{test} ) {
		delete $self->{__DELETING__};
		return;
	}
	unless (exists $self->{__OFFLINING__} && $self->{__OFFLINING__}) {
		$self->saveHistory("delete offline $id where datastamp in $date and " .
			"$id from ".$index->{$date}{id_min}." to ".($index->{$date}{id_max}) );
	}
	delete $index->{$date};
	$self->writeIndex($index);
	delete $self->{__DELETING__};
}

=item I<listOfflineProcess($quiet)>

 list offliMakss process that is list the content of the index file

Use B<$quiet> to have only the list of the months as YYYYMM in offline state. You can use this as pipe to online to database the log
 
=cut

sub listOfflineProcess 
{
	my $self=shift;
	my $quiet=shift;
	my ($index,$idx);
	my $session=$self->{session};
	# save on $self->{__LISTING__} the YYYYMM that is in working progress
	$self->{__LISTING__}='';
	return unless $self->checkLogDir();
	$index=$self->readIndex();
	unless ($quiet) {
		print sprintf('+ %s + %s + %s + %s + %s + %s +'."\n",'------','--------','--------','--------','---------','---------');
		print sprintf('| %s | %s | %s | %s | %s | %s |'."\n",'YYYYMM','   TOT  ','DWNLOADS','  VIEWS ','  ID_MIN ','  ID_MAX ');
		print sprintf('+ %s + %s + %s + %s + %s + %s +'."\n",'------','--------','--------','--------','---------','---------');
	}
	foreach $idx (sort keys %$index) {
		if ($quiet) {print $idx."\n"; next; }
		print sprintf('| %06d | % 8d | % 8d | % 8d | % 9d | % 9d |'."\n",
			$idx,
			$index->{$idx}{tot},
			$index->{$idx}{download},
			$index->{$idx}{view},
			$index->{$idx}{id_min},
			$index->{$idx}{id_max} );
	}
	print sprintf('+ %s + %s + %s + %s + %s + %s +'."\n",'------','--------','--------','--------','---------','---------') unless $quiet;
	delete $self->{__LISTING__};
}

1;

=back

=head1 COPYRIGHT

    Offline::Handler is Copyright (c) 2014 Enio Carboni - Italy
    This file is part of Offlinelogs.

    Offline::Handler is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Offline::Handler is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with Offline::Handler.  If not, see <http://www.gnu.org/licenses/>.

=head1 SUPPORT / WARRANTY

The Offlinelogs is free Open Source software. IT COMES WITHOUT WARRANTY OF ANY KIND.
