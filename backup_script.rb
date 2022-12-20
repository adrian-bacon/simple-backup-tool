#!/usr/bin/env ruby

require 'json'

# Get our global variables done.
now = Time.now
touch = now.strftime("%Y%m%d%H%M.%S")

backup_configuration = JSON.parse(File.read("backup_configuration.json"))

backup_devices = backup_configuration['backup_devices']
file_paths = backup_configuration['file_paths']
cmd = ""

# Check to see if we're already running a backup job.
#
begin
  if (File.exist?("/tmp/backup_in_progress"))
    File.write("/var/log/backup_log.txt", "#{Time.now} backup already in progress, skipping this backup run.\n", :mode=>"a")
    exit
  else
    system("touch /tmp/backup_in_progress")
    system("rm -f /var/log/backup_log.txt.last")
    system("mv /var/log/backup_log.txt /var/log/backup_log.txt.last")
    system("touch /var/log/backup_log.txt")
    File.write("/var/log/backup_log.txt", "#{Time.now} starting backup\n", :mode=>"a")
  end

  # Step through the file systems and back each one up.
  #
  file_paths.each do |file_path|

    tar_name = file_path['tar_name']

    # snapshot the file system
    #
    if (file_path['zfs'])
      File.write("/var/log/backup_log.txt", "#{Time.now} create snapshot for #{file_path['filesystem']}\n", :mode=>"a")
      system("zfs snapshot #{file_path['pool']}#{file_path['filesystem']}@now")
    end

    File.write("/var/log/backup_log.txt", "#{Time.now} backing up #{file_path['path']}\n", :mode=>"a")

    # Step through each backup device and back up the file system to it.
    #
    file_path['backup_devices'].each do |device|
      backup_device = backup_devices[device]['zfs_file_system']
      backup_path = backup_devices[device]['target']
      partition = backup_devices[device]['partition']
      wday_of_full_backup = backup_devices[device]['wday_of_full_backup']
      offsite = backup_devices[device]['offsite']
      cold_storage = backup_devices[device]['cold_storage']

      # Check to see if the drive is attached
      #
      if (!File.exist?(partition))
        File.write("/var/log/backup_log.txt", "#{Time.now} Skipping #{backup_device}, drive not attached.\n", :mode=>"a")
        next
      end

      # Mount the backup device.
      #
      File.write("/var/log/backup_log.txt", "#{Time.now} mounting #{backup_device} to #{backup_path}\n", :mode=>"a")
      system("zpool import #{backup_device}")

      # Check to see if it actually is mounted.
      #
      if (File.exist?(backup_path + "/mounted"))
        File.write("/var/log/backup_log.txt", "#{Time.now} #{backup_device} mounted to #{backup_path}\n", :mode=>"a")

        if (!Dir.exist?(backup_path + "/tars"))
          File.write("/var/log/backup_log.txt", "#{Time.now} creating #{backup_path}/tars/\n", :mode=>"a")
          system("cd #{backup_path}; mkdir tars")
        end

        # do our backup logic here
        #
        if ((now.wday == wday_of_full_backup) || (cold_storage == true))
          # We have different behavior for wday_of_full_backup backups because they are a full backup.
          #
          File.write("/var/log/backup_log.txt", "#{Time.now} doing full backup of #{file_path['path']}\n", :mode=>"a")

          if (file_path['zfs'])
            # we're backing up a zfs dataset
            cmd = "cd #{file_path['path']}/.zfs/snapshot/now; tar -cf #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"

          else
            # we're backing up a path
            cmd = "cd #{file_path['path']}; tar -cf #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"
          end

          # force remove the old backup
          #
          File.write("/var/log/backup_log.txt", "#{Time.now} removing #{backup_path}/tars/#{tar_name}\n", :mode=>"a")
          system("rm -f #{backup_path}/tars/#{tar_name}; sync")

        else
          # do an incremental backup
          #
          File.write("/var/log/backup_log.txt", "#{Time.now} doing incremental backup of #{file_path['path']}\n", :mode=>"a")
          if (File.exist?("#{backup_path}/tars/#{tar_name}"))
            if (file_path['zfs'])
              # we're backing up a zfs dataset
              cmd = "cd #{file_path['path']}/.zfs/snapshot/now; tar -rf #{backup_path}/tars/#{tar_name} --newer-mtime-than #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"

            else
              # we're backing up a path
              cmd = "cd #{file_path['path']}; tar -rf #{backup_path}/tars/#{tar_name} --newer-mtime-than #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"
            end

          else
            if (file_path['zfs'])
              # we're backing up a zfs dataset
              cmd = "cd #{file_path['path']}/.zfs/snapshot/now; tar -cf #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"

            else
              # we're backing up a path
              cmd = "cd #{file_path['path']}; tar -cf #{backup_path}/tars/#{tar_name} * 2>> /var/log/backup_log.txt"
            end
          end
        end

        # execute the backup
        #
        File.write("/var/log/backup_log.txt", "#{Time.now} executing: #{cmd}\n", :mode=>"a")
        system(cmd)
        system("touch -t #{touch} #{backup_path}/tars/#{tar_name}")

        # unmount the backup device
        #
        File.write("/var/log/backup_log.txt", "#{Time.now} unmounting #{backup_device} from #{backup_path}\n", :mode=>"a")
        system("sync; zpool export #{backup_device}")

      else
        File.write("/var/log/backup_log.txt", "#{Time.now} !!!! #{backup_device} does not appear to be mounted to #{backup_path}\n", :mode=>"a")

        # As a safety, lets issue an unmount just in case
        #
        system("sync; zpool export #{backup_device}")
      end

    end # END file_path['backup_devices'].each do |device|

    # destroy the snapshot
    #
    if (file_path['zfs'])
      File.write("/var/log/backup_log.txt", "#{Time.now} destroy snapshot for #{file_path['filesystem']}\n", :mode=>"a")
      system("zfs destroy #{file_path['pool']}#{file_path['filesystem']}@now")
    end

  end # END file_paths.each do |file_path|

rescue Exception => e
  File.write("/var/log/backup_log.txt", "#{Time.now} \n\n\n!!!! An exception occurred\n\n#{e.inspect}\n\n\n", :mode=>"a")

ensure

  # Ensure all our backup drives are unmounted
  #
  backup_devices['umounts'].each do |umount|
    system("sync; zpool export #{backup_devices[umount]['zfs_file_system']}")
  end

  system("rm -f /tmp/backup_in_progress")

  File.write("/var/log/backup_log.txt", "#{Time.now} finished backing up\n", :mode=>"a")

  ### EMAIL BACKUP RESULTS HERE

end

