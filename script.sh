for i in {1..5}
do
    osascript -e "tell application \"Terminal\" to do script \"cd `pwd`/src/part1/client && python3 client.py\""
done
