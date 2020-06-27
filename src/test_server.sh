# test_server.sh
#
# run some commands against the server

test_create() {
    echo -e "\ncreate"
    curl localhost:8080/index -XPOST -H 'content-type:application/json' -d '
    {
        "command": "create",
        "docId": "__test.txt",
        "text": "hello worlds!"
    }
'
}

test_update() {
    echo -e "\nupdate"
    curl localhost:8080/index -XPOST -H 'content-type:application/json' -d '
    {
        "command": "update",
        "docId": "__test.txt",
        "docs": "goodbye worlds!"
    }
'
}

test_read() {
    echo -e "\nread"
    curl "localhost:8080/index?docId=*"
}

test_delete() {
    echo -e "\ndelete"
    curl -XDELETE "localhost:8080/index?docId=$1" 
}

test_multi_update() {
    echo -e "\nmulti_update"
    reply=$(curl localhost:8080/index -XPOST -H 'content-type:application/json' -d '
    {
        "command": "update",
        "docId": "__test.txt",
        "docs": ["goodbye worlds!", "you a bad mamma jamma"]
    }
' | python -c 'import json, sys; print(" ".join(json.load(sys.stdin)["docIds"]))')
    for file in $reply; do
        test_delete "$file"
    done
}

case "$1" in
    '-c') test_create ;;
    '-u') test_update ;;
    '-d') test_delete "__test.txt" ;;
    '-m') test_multi_update ;;
    '-r') test_read ;;
esac

echo -e "\ndone"
