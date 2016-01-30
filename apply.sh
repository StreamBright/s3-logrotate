for file in $(find src -type f); do
  echo $file
  if ! grep -q 'Copyright' $file
  then
   cat HEADER $file > $file.new && mv $file.new $file
  fi
done
