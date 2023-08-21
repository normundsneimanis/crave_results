## Installation instructions

    sudo apt-get install mariadb-server python3-cryptography python3-posix-ipc python3-dotenv
    sudo mysql_secure_installation

    # Allow connection from any host
    # /etc/mysql/mariadb.conf.d/50-server.cnf
    # bind-address            = 0.0.0.0
    sudo service mariadb restart

    sudo apt-get install phpmyadmin
    # [x] apache2
    # Access via http://localhost/phpmyadmin
    sudo apt-get install libapache2-mod-php8.1 libapache2-mod-php
    sudo service apache2 restart

    # Log in database as root
    sudo mysql
    
    # Create CraveResults user
    CREATE USER 'craveresults'@'%' IDENTIFIED BY 'password';
    GRANT ALL PRIVILEGES ON craveresults.* TO 'craveresults'@'%';
    flush privileges;
    create database craveresults;

    # Create superuser
    CREATE USER 'normunds'@'%' IDENTIFIED BY 'password';
    GRANT ALL PRIVILEGES ON *.* TO 'normunds'@'%' WITH GRANT OPTION;
    GRANT ALL PRIVILEGES ON database_name.* TO 'normunds'@'%';
    flush privileges;

    # Change password
    ALTER USER 'normunds'@'%' IDENTIFIED BY 'new_password';

    # Remove user
    REVOKE ALL PRIVILEGES, GRANT OPTION FROM 'root2'@'%';
    DROP USER 'root2'@'%';
    flush privileges;

    # Install python dependencies
    python3 -m pip install hyperopt

    # Configure open files limit, /etc/security/limits.conf
    <user>      hard nofile 8192
    <user>      soft nofile 4096

    # Configure server access with .env
    cp .env.example .env

    # Make save directories
    mkdir /var/lib/crave_results_db
    mkdir /var/lib/crave_results_db/files
    mkdir /var/lib/crave_results_db/hyperopt
    mkdir /var/lib/crave_results_db/shared_status
    chown -R <user> /var/lib/crave_results_db
    ln -s <dataset base directory> /var/lib/crave_results_db/dataset
    sudo cp tools/startup-scripts/crave-results-server.service /etc/systemd/system
    sudo systemctl daemon-reload
    sudo systemctl enable crave-results-server.service
    sudo systemctl start crave-results-server.service
    sudo systemctl status crave-results-server

    # Install mariadb connector from the top of readme.md
