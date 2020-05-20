mountpoint = "/tmp/plop"
default_volume = "example"

chunker {
  min = 512 * KiB
  max = 2 * MiB
  average = 1 * MiB
}

volume "example" {
  passphrase = "correct horse battery stable"
  bucket {
    url = "file:///tmp/plopfs-demo"
  }
}

volume "another" {
  passphrase = "slartibartfast"
  bucket {
    url = "file:///tmp/plopfs-demo"
  }
  chunker {
    min = 1 * MiB
    max = 10 * MiB
    average = 4 * MiB
  }
}
