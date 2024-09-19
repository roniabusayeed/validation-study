### Setup (macOS or Linux)

1. Open your terminal and navigate to your home directory.
    ```shell
    cd ~
    ```
2. Identify which shell you're using.
    ```shell
   echo $SHELL
    ```
   You should get an output is `/bin/bash` or `/usr/bin/bash`, you're using `bash`. And if you're output
   is like `/bin/zsh` or `/bin/zsh`, you're using `zsh`.

3. If you're using `bash`, you should have a file called `.bashrc` or `.profile`, or if you're using `zsh`,
you should have a file called `.zshrc` under your home directory. Open any of the files for `bash` or the
file for `zsh` into a text editor and add the following lines at the end of the file.
    ```shell
   export LOKI_USERNAME=<your-username-for-loki>                # Replace <...> with your actual username for loki server.
   # Example: export LOKI_USERNAME="john"
   
   export LOKI_PASSWORD=<your-password-for-loki>                # Replace <...> with your actual user password for loki server.
   # Example: export LOKI_PASSWORD="password"
   
   export STAGING_DB_USERNAME=<your-username-for-stating_db>    # Replace <...> with your actual database username for staging_db.
   export STAGING_DB_PASSWORD=<your-password-for-staging_db>    # Replace <...> with your actual database password for staging_db.
    ```
    Save and close the file.
4. In your terminal enter the following command.
    ```shell
   source <filename>  # Replace <filename> with either .bashrc/.profile/.zshrc based on your shell (refer to step 3).
   ```
5. Navigate to a different directory (if you want) where you want to save the code (e.g., `cd <destination-directory>`),
clone the repository and prepare python virtual environment.
    ```shell
   git clone https://github.com/roniabusayeed/validation-study.git
   cd validation-study
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
    ```
6. In the same terminal (i.e., while the virtual environment is still activated—your prompt will reflect it), launch
Jupyter server.
    ```shell
   jupyter lab
    ```
   
Enjoy running the jupyter notebooks!