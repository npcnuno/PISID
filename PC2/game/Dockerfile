FROM scottyhardy/docker-wine:latest

# Update package lists and install python3 and python3-pip
RUN apt-get update && apt-get install -y python3 python3-pip

# Set environment variables for Wine
ENV WINEPREFIX=/home/wineuser/.wine
ENV WINEARCH=win64

# Create Wine prefix directory with correct ownership
RUN mkdir -p $WINEPREFIX

# Switch to wineuser and set HOME
ENV HOME=/home/wineuser

# Copy and install Python requirements
COPY requirements.txt /app/requirements.txt
COPY mqtt_to_mongodb.py /app/mqtt_to_mongodb.py
RUN pip install --no-cache-dir -r /app/requirements.txt

# Set XDG_CACHE_HOME explicitly
ENV XDG_CACHE_HOME=$HOME/.cache

# Initialize Wine prefix by running a simple command
RUN wine cmd.exe /c exit

# Install necessary components
RUN wine msiexec /i https://dl.winehq.org/wine/wine-mono/8.1.0/wine-mono-8.1.0.msi /q || true
RUN wine msiexec /i https://dl.winehq.org/wine/wine-gecko/2.47.3/wine_gecko-2.47.3-x86.msi /q || true
RUN winetricks -q win10 || true
RUN winetricks -q vcrun2015 || true

# Set working directory and copy your application files
WORKDIR /app
COPY entrypoint.sh /app/entrypoint.sh
COPY game.exe /app/game.exe
# Copy the _internal folder (ensure it contains python311.dll)
COPY _internal /app/_internal

# Make sure the entrypoint script is executable
RUN chmod +x /app/entrypoint.sh

# Use our custom entrypoint
ENTRYPOINT ["/app/entrypoint.sh"]

