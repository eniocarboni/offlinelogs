$c->{offline}= {
	# logdir: directory (must exist) where we save the access log in offline state
	#  if empty we use $repo->get_conf('variables_path')."/access_offline"
	logdir=>'',
	# months_to_keep: are the months to keep online from now in access table (def. 6)
	months_to_keep=>6,
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
