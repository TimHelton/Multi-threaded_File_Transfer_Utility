#!/usr/bin/perl
#
# Project		:	PIN PREPINIA 
# Description		:	This program copies files from a remote machine and places them
#				on a local machine and in an archive directory on the remote
#				machine.
# Warning		:	Don't Change any part of this code unless discussed with Database 
# 				Engineering Team.
# Purpose		:	This is for All client FTP files, which needs to be processed.
# Date			:	17NOV2016.
# Author		:	Tim Helton - 
# Modified		:	Additional log informations are updated for report purpose.
#				3/8/17 - Adds support for running program as multiple
#				processes

################################################################################################
# This script performs an ftp operation in which it retrieves files from a 
# remote server and places them on a local server. It takes the following options:
################################################################################################

use lib "/d01/apps/perl/lib";

use strict;
use warnings;
use Net::SFTP::Foreign;
use File::Path qw/mkpath/;
use POSIX ":sys_wait_h";
use Jdpa::Log;

################################################################################################
# Global variables
################################################################################################

my %Config = ();
my @Clients = ();
my @ClientsFromConfig = ();
my $CredentialsFile = "";
my $Debug = "";
my $Errors = 0;
my $LogPath = "";
my $Password = "";
my $PropFile = "";		
my $RemoteBasePath= "";
my $RemotePort= "";
my $RemoteServer= "";
my $TargetBasePath= "";
my $User = "";

my $MyName = basename($0, '.pl');

&main();

################################################################################################
# Main Routine: This routine takes no arguments and returns nothing useful. It controls the
# program flow.
################################################################################################

sub main {

	&init();

	# Fork each set of clients as a separate process
	########################################################################################
	my $i;
	for ($i = 0; $i < @Clients; ++$i) {
		my $pid;
		if ($pid = fork) {
			print("Forking child processes.\n");
		} elsif (defined $pid) {
			close(STDOUT);
			close(STDERR);
			close(STDIN);
			doOneProcess($i);
		} else {
			die "Can't fork a child process.";
		}
		print "Process $$ spawned process $pid for clients line $i.\n";
	}

	# Wait for all child processes to terminate to avoid zombies
	########################################################################################
	my $child;
	do {	
		$child = waitpid(-1, &WNOHANG);
	} until $child == -1;
}

################################################################################################
# DoOneProcess takes a client index as its only argument. It returns nothing useful. It 
# instantiates a log for the process and runs getFiles for the clients specified.
################################################################################################

sub doOneProcess {

	my($clientIndex) = @_;

	my $log = Log->new($MyName . "_" . $$, $LogPath) || die "Can't instantiate log object.\n";

	$log->writeLog("$Clients[$clientIndex] being handled by process $$", 0);

	&getFiles($log, $clientIndex); 

	$log->endLog();
}

################################################################################################
# Init takes no arguments and returns nothing useful. It calls the following subroutines:
# 	-parseArguments parses the command line arguments.
# 	-getConfig reads the configuration files.
################################################################################################

sub init {
	
	use File::Basename;

	my $message;
	
	&parseArguments();
	
	# If the -p flag is passed use the specified configuration file. Otherwise, use
	# the default
	unless ($PropFile) {
		$PropFile = "/d02/scripts/dmsFtpProperties.prop";
	}

	# If the -c flag is passed use the specified configuration file. Otherwise, use
	# the default
	unless ($CredentialsFile) {
		$CredentialsFile = "/d02/scripts/dmsFtpCredentials.prop";
	}

	# Load the Configuration hash
	&getConfig($PropFile); 
	&getConfig($CredentialsFile); 

	# If the -l flag is passed use the specified log path. Otherwise, use
	# the log path specified in the FTP properties file
	unless ($LogPath) {
		$LogPath = $Config{"LOG_PATH"};
	}

	if ($LogPath) {
		if ( !-d $LogPath ) {
			mkdir $LogPath;
		}
	} else {
		$message = "No LogPath specified.\n";
		print STDERR $message;
		helpMessage();
		}
	
	# If the -p flag is passed use the specified port. Otherwise, use
	# the port specified in the FTP properties file
	unless ($RemotePort) { 
		$RemotePort = $Config{"REMOTE_PORT"};
	}

	if ($RemotePort) {
		$message = "Remote port is $RemotePort.\n";
		print $message;
	} else {
		$message = "No remote port specified.\n";
		print $message;
		exit -1;
	}
	
	# If the -s flag is passed use the specified server name. Otherwise, use
	# the server specified in the FTP properties file
	unless ($RemoteServer) { 
		$RemoteServer = $Config{"REMOTE_SERVER_NAME"};
	}

	if ($RemoteServer) {
		$message = "Remote server is $RemoteServer.\n";
		print $message;
	} else {
		$message = "No remote server specified.\n";
		print $message;
		exit -1;
	}
	
	# If the -C flag is passed use the specified client names. Otherwise, use
	# the client specified in the FTP properties file
	unless (@Clients) {
		@Clients = @ClientsFromConfig;
	}	
	
	if (@Clients) {
		$message = "Clients are ";
		foreach my $client (@Clients) {
			$message .= $client . ",";
			}
		$message .= "\n";
		print $message;
	} else {
		$message = "No clients specified.\n";
		print $message;
		exit -1;
		}
	
	# If the -r flag is passed use the specified remote path. Otherwise, use
	# the remote path specified in the FTP properties file
	unless ($RemoteBasePath) {
		if ($Config{"REMOTE_BASE_PATH"}) {
			$RemoteBasePath = $Config{"REMOTE_BASE_PATH"};
		}
	}	
	
	if ($RemoteBasePath) {
		$message = "RemoteBasePath is $RemoteBasePath.\n";
		print $message;
	} else {
		$message = "No RemoteBasePath specified.\n";
		print $message;
		exit -1;
		}
	
	# If the -t flag is passed use the specified target path. Otherwise, use
	# the target path specified in the FTP properties file
	unless ($TargetBasePath) {
		if ( $Config{"TARGET_BASE_PATH"} ) {
			$TargetBasePath = $Config{"TARGET_BASE_PATH"};
		}
	}	

	if ($TargetBasePath) {
		$message = "TargetBasePath is $TargetBasePath.\n";
		print $message;
	} else {
		$message = "No TargetBasePath specified.\n";
		print $message;
		exit -1;
		}

	unless ($User) {
		$User = $Config{"USER"};
	}

	if ($User) {
		$message = "User is $User.\n";
		print $message;
	} else {
		$message = "No FTP user specified.\n";
		print $message;
		exit -1;
		}

	unless ($Password) {
		$Password = $Config{"PASSWORD"};
	}

	unless($Password) {
		$message = "No FTP password specified.\n";
		print $message;
		exit -1;
		}
	
	if ($Debug) {
		&printGlobals();
		exit;
	}	
}

################################################################################################
# parseArguments takes no arguments and returns nothing useful. It reads the command line 
# options and sets various variables as appropriate
################################################################################################

sub parseArguments {

	use Getopt::Long;
	use vars qw($arguments $opt_help $Verbose);

	GetOptions("help",
			"a=s" => \$CredentialsFile,
			"c=s" => \@Clients,
			"d"   => \$Debug,
			"f=s" => \$PropFile,
			"l=s" => \$LogPath,
			"r=s" => \$RemoteBasePath,
			"s=s" => \$RemoteServer,
			"t=s" => \$TargetBasePath,
			"u=s" => \$User,
			"w=s" => \$Password
			);

	&helpMessage() if $opt_help;

}

################################################################################################
# printGlobals is called only if the debug option is passed. It prints the options.
################################################################################################

sub printGlobals {

	print "CredentialsFile = $CredentialsFile\n";

	print "Clients include\n";
	foreach my $client (@Clients) {
		print "\t$client\n";
	}
	
	print "Debug is $Debug\n";

	print "LogPath = $LogPath.\n";

	if ($PropFile) {
		print "PropFile = $PropFile.\n";
	} else {
		print "PropFile is not set.\n";
	}

	print "RemoteBasePath = $RemoteBasePath\n";
	
	print "RemoteServer = $RemoteServer\n";

	print "TargetBasePath = $TargetBasePath\n";
	
	print "User = $User\n";
}

################################################################################################
# getConfig takes the name of a properties file as its only argument. It returns nothin useful.
# It stores its information in the global %Config hash.
################################################################################################

sub getConfig {

	my ($filename) = @_;

	open (CONFIG, "$filename") or die  "DMS Property file missing : $filename. \n";

	my $line;

    	while ($line = <CONFIG>) {
        	chop ($line);               # Remove trailing \n
        	$line =~ s/^\s*//;          # Remove spaces at the start of the line
        	$line =~ s/\s*$//;          # Remove spaces at the end of the line

        	if ( ($line !~ /^#/) && ($line ne "") ){    # Ignore lines starting with and blank 
							    # lines
								   
            		my ($name, $value) = split (/=/, $line);      	# Split each line into 
						  		   	# name value pairs
			$name =~ tr/[a-z]/[A-Z]/;   # Forgive lowercase configuration names
		
			if ($name eq "CLIENTS") {
				push @ClientsFromConfig, $value;
			} else {
        			$Config{$name} = $value;  # Add value to hash for specified name
    			}
		}
	}
	close(CONFIG);
}

################################################################################################
# getFiles takes no arguments and returns nothing useful. It opens an SFTP  connection, 
# navigates# to the remote path and obtains a list of files for processing. Foreach of these 
# files, it retrieves the file and places in the target path on the local machine. Unless the 
# -l option is used, getFiles next copies the file to the backup directory on the remote 
# machine and removes the remote file.
################################################################################################

sub getFiles {

	my ($log, $clientIndex) = @_;

	my @clients = split(',', $Clients[$clientIndex]);

	my $sftp = Net::SFTP::Foreign->new($RemoteServer, port=>22, 
					   user=>$User, password=>$Password) || 
		die "Can't connect to $RemoteServer.";

	my $client;
	foreach $client (@clients) {
		my $message = "Processing begun for $client.\n";
		$log->writeLog($message, 0);

		my $targetPath = $TargetBasePath . "/" . $client . "/ftp_in";
		if (-d $targetPath) {
			$message = "Downloading to $targetPath.\n";
			$log->writeLog($message, 0);
		} else {
			$message = "Creating missing directory $targetPath for download.\n";
			$log->writeLog($message, 0);
		
			unless (File::Path::mkpath($targetPath)) {
				$message = "Can't create $targetPath.\n";
				$log->writeLog($message, 1);
				next;
			}
		}

		my $remotePath = $RemoteBasePath . "/" . $client . "/data";
		my $archivePath = $RemoteBasePath . "/" . $client . "/archive";

		unless ($sftp->setcwd("$remotePath")) {
			$message = "Can't cwd to $remotePath on $RemoteServer. $!\n";
			$log->writeLog($message, 1);
			$message = "FTP Error was " . $sftp->error() . "\n";
			$log->writeLog($message, 0);
			next;
		}

		my @remoteFiles;
		unless (@remoteFiles = @{$sftp->ls($remotePath, names_only=>1)}) {
			$message = "There are no files in $remotePath\n";
			$log->writeLog($message, 0);
			}

		my $file; 
		foreach $file (@remoteFiles) {

			# Get the file
			#######################################################################
			$message = "Downloading $file to $targetPath.\n";
			$log->writeLog($message, 0);

			if($sftp->get($file, "$targetPath/$file")) {

				# If the archive directory doesn't exist, create it.
				###############################################################
				unless($sftp->test_d($archivePath)) {
					$message = "Creating remote directory $archivePath.\n";
					$log->writeLog($message, 0);

					unless($sftp->mkdir($archivePath)) {
						$message = "Can't create $archivePath.\n";
						$log->writeLog($message, 1);
						$message = "FTP Error was " . $sftp->error() . "\n";
						$log->writeLog($message, 0);
						last;
					}
				}

				# Archive the remote file
				###############################################################
				$message = "Archiving $file to $archivePath.\n";
				$log->writeLog($message, 0);

				unless($sftp->rename($file, "$archivePath/$file")) {
					$message = "Can't rename $file to " . 
							"$archivePath/$file.\n";
					$log->writeLog($message, 1);
					$message = "FTP Error was " . $sftp->error() . "\n";
					$log->writeLog($message, 0);
					}
			} else {
				$message="Couldn't get $file.";
				$log->writeLog($message, 1);
			}
		}
	}
}

################################################################################################
# helpMessage takes no arguments. It displays a help message and then exits the program.
################################################################################################

sub helpMessage {

	print <<END_HELP;

$MyName

Syntax

	$MyName [-a credentials_file -c client_list [ -c client_list ... ] -d
		 -f properties_file -l log_path -p port -r remote_path -s remote_server
		 -t target_path -u ftp_user -w password]

This script performs an ftp operation in which it retrieves files from a remote server
and places them on a local server. It takes the following options which take precedence over 
options set in the configuration files:

	-a	The name of the credentials file. If this option is not specified, the 
		credentials are read from /d02/scripts/dmsFtpCredentials.prop.
 	-c	If this option is used, the files for the clients passed as arguments are 
 		retrieved. Otherwise the files associated with a list of clients read from a 
 		properties file are retrieved. Multiple -c flags can be used, and when they
		are, a separate process is forked for the series of clients specified with
		each flag.
	-d 	Run in debug mode.
	-f 	The name of the properties file. If this option is not specified, the
		properties are read from /d02/scripts/dmsProperties.prop.
	-l 	The path to the log files. If this option is not specified, the logs are
		written to the path specified in the FTP properties file.
	        file is transferred to a backup directory after retrieval.
	-p	The port number on which to reach the remote machine.
	-r	The path on the remote machine. If this is not specified, the remote path
		is read from the properties file.
	-s 	The name of the remote server. If this option is not specified, the server name
		is read from the properties file.
	-t	The path on the local machine. If this is not specified, the target path
		is read from the properties file.
	-u 	The name of the ftp user to be logged on.
	-w	The password to be used for ftp log on.

END_HELP

	exit;

}
