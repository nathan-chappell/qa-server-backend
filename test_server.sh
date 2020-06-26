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

test_delete() {
    echo -e "\ndelete"
    curl -XDELETE localhost:8080/index?docId='__test.txt' 
}

case "$1" in
    '-c') test_create ;;
    '-u') test_update ;;
    '-d') test_delete ;;
esac

echo -e "\ndone"
