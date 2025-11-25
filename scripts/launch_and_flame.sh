java -jar target/Anonymization_EA_CEP_thesis-1.0-SNAPSHOT-jar-with-dependencies.jar -v -nt 10 -f experiment.txt &
echo "PID = $!"
echo "Waiting 20 seconds for the system to start up..."
sleep 20
~/async-profiler-4.2.1-linux-x64/bin/asprof -e wall -d 30 -f wall.html $!
echo "Profiling complete. Output saved to profile.html. Killing the Java process."
kill $!