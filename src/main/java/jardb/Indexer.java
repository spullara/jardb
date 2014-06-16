package jardb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.async.ReadyFuture;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import org.apache.maven.model.Dependency;
import org.apache.maven.model.DependencyManagement;
import org.apache.maven.model.Model;
import org.apache.maven.model.Parent;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.LongAdder;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Indexer {

  private static final byte[] EMPTY_BYTES = new byte[0];

  @Argument
  private boolean skipJars = false;

  @Argument(alias = "d", description = "Specify the directory to index")
  private File directory;

  @Argument(alias = "g", description = "Group id")
  private String groupId;

  @Argument(alias = "a", description = "Artifact id")
  private String artifactId;

  @Argument(alias = "v", description = "Version")
  private String version;

  @Argument(alias = "f", description = "Class file name")
  private String classFile;

  @Argument(alias = "c", description = "Class name")
  private String className;

  @Argument(alias = "p", description = "Package name")
  private String packageName;

  private FDB fdb = FDB.selectAPIVersion(200);
  private Database db = fdb.open();
  private DirectoryLayer dl = DirectoryLayer.getDefault();
  private DirectorySubspace jardb = dl.createOrOpen(db, asList("jardb")).get();
  private Subspace metadata = jardb.get("metadata");
  private DirectorySubspace artifacts = jardb.createOrOpen(db, asList("artifacts")).get();
  private DirectorySubspace artifactsByGroupId = artifacts.createOrOpen(db, asList("group")).get();
  private DirectorySubspace artifactsByArtifactId = artifacts.createOrOpen(db, asList("artifact")).get();
  private DirectorySubspace artifactsByPackage = artifacts.createOrOpen(db, asList("package")).get();
  private DirectorySubspace artifactsByClass = artifacts.createOrOpen(db, asList("class")).get();
  private DirectorySubspace artifactsByDependencies = artifacts.createOrOpen(db, asList("dependencies")).get();
  private DirectorySubspace dependenciesByArtifacts = artifacts.createOrOpen(db, asList("dependee")).get();
  private DirectorySubspace classes = jardb.createOrOpen(db, asList("classes")).get();
  private DirectorySubspace classesBySHA = classes.createOrOpen(db, asList("sha")).get();
  private DirectorySubspace classesByClassname = classes.createOrOpen(db, asList("name")).get();

  public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
    Indexer indexer = new Indexer();
    Args.parseOrExit(indexer, args);
    indexer.run();
  }

  private void run() throws IOException {
    if (directory != null) {
      index();
    } else {
      lookup();
    }
  }

  public void lookup() {
    if (className != null) {

    } else if (classFile != null) {

    } else if (packageName != null) {

    } else if (groupId != null) {
      List<Artifact> artifacts = db.run(new Function<Transaction, List<Artifact>>() {
        @Override
        public List<Artifact> apply(Transaction tx) {
          Subspace subspace = artifactsByGroupId.get(groupId);
          if (artifactId != null) {
            subspace = subspace.get(artifactId);
            if (version != null) {
              subspace = subspace.get(version);
            }
          }
          return StreamSupport.stream(tx.getRange(Range.startsWith(subspace.pack())).spliterator(), false)
                  .map(kv -> {
                    Tuple key = artifactsByGroupId.unpack(kv.getKey());
                    return new Artifact(key.getString(0), key.getString(1), key.getString(2));
                  })
                  .filter(a -> a.get(0).equals(groupId) &&
                          (artifactId == null || a.get(1).equals(artifactId)) &&
                          (version == null || a.get(2).equals(version)))
                  .collect(Collectors.toList());
        }
      });
      for (Artifact artifact : artifacts) {
        System.out.println(artifact.get(0) + ", " + artifact.get(1) + ", " + artifact.get(2));
      }
    } else if (artifactId != null) {
      List<Artifact> artifacts = db.run(new Function<Transaction, List<Artifact>>() {
        @Override
        public List<Artifact> apply(Transaction tx) {
          Subspace subspace = artifactsByArtifactId.get(artifactId);
          if (version != null) {
            subspace = subspace.get(version);
          }
          return StreamSupport.stream(tx.getRange(Range.startsWith(subspace.pack())).spliterator(), false)
                  .map(kv -> {
                    Tuple key = artifactsByArtifactId.unpack(kv.getKey());
                    return new Artifact(key.getString(0), key.getString(1), key.getString(2));
                  })
                  .filter(a -> (groupId == null || a.get(1).equals(groupId)) &&
                          (artifactId == null || a.get(0).equals(artifactId)) &&
                          (version == null || a.get(2).equals(version)))
                  .collect(Collectors.toList());
        }
      });
      for (Artifact artifact : artifacts) {
        System.out.println(artifact.get(0) + ", " + artifact.get(1) + ", " + artifact.get(2));
      }
    }
  }

  public void index() throws IOException {
    if (!directory.exists()) {
      System.err.println("Directory " + directory + " doesn't exist");
      System.exit(1);
    }
    LongAdder count = new LongAdder();
    LongAdder error = new LongAdder();
    LongAdder sets = new LongAdder();

    byte[] currentIdLocation = metadata.get("currentId").pack();

    LoadingCache<Artifact, byte[]> artifactAbbreb = CacheBuilder.newBuilder().build(new CacheLoader<Artifact, byte[]>() {
      @Override
      public byte[] load(Artifact artifact) throws Exception {
        return db.run(new Function<Transaction, byte[]>() {
          @Override
          public byte[] apply(Transaction tx) {
            byte[] artifactToIdsKey = metadata.get("artifactToId").get(artifact.get(0)).get(artifact.get(1)).get(artifact.get(2)).pack();
            byte[] currentId = tx.get(artifactToIdsKey).get();
            if (currentId == null) {
              currentId = tx.get(currentIdLocation).get();
              if (currentId == null) {
                tx.set(currentIdLocation, currentId = Tuple.from(1l).pack());
              } else {
                tx.set(currentIdLocation, currentId = Tuple.from(Tuple.fromBytes(currentId).getLong(0) + 1l).pack());
              }
              tx.set(metadata.get("idToArtifact").get(currentId).pack(), Tuple.from(artifact.get(0), artifact.get(1), artifact.get(2)).pack());
              tx.set(artifactToIdsKey, currentId);
            }
            return currentId;
          }
        });
      }
    });

    Files.walk(directory.toPath()).parallel().filter(p -> p.toString().endsWith(".pom")).forEach(s -> {
      MavenXpp3Reader reader = new MavenXpp3Reader();
      try {
        BufferedInputStream in = new BufferedInputStream(new FileInputStream(s.toFile()));
        Model read = reader.read(in);
        in.close();
        String groupId = group(read);
        String artifactId = artifact(read);
        String version = version(read);
        Path parentDir = s.getParent();
        if (read.getPackaging().equals("jar")) {
          String name = s.toFile().getName();
          Path jarPath = parentDir.resolve(name.substring(0, name.length() - 4) + ".jar");
          File file = jarPath.toFile();
          if (file.exists()) {
            System.out.println("Processing " + file + " " + count + " " + error + " " + sets);
            byte[] abbrev = artifactAbbreb.getUnchecked(new Artifact(groupId, artifactId, version));
            List<Dependency> dependencies = read.getDependencies();
            DependencyManagement dm = read.getDependencyManagement();
            Map<List<String>, String> dependencyManagement = dm == null ? Collections.emptyMap() :
                    dm.getDependencies().stream()
                    .collect(toMap(d -> asList(d.getGroupId(), d.getArtifactId()), Dependency::getVersion));
            List<Artifact> dependentArtifacts = dependencies.stream().map(d -> {
              String gid = d.getGroupId();
              String aid = d.getArtifactId();
              String v = d.getVersion();
              if (v == null) {
                v = dependencyManagement.get(asList(gid, aid));
              }
              return new Artifact(gid, aid, v);
            }).collect(toList());
            db.runAsync(new Function<Transaction, Future<Void>>() {
              @Override
              public Future<Void> apply(Transaction tx) {
                dependentArtifacts.forEach(a -> {
                  byte[] dependencyAbbrev = artifactAbbreb.getUnchecked(a);
                  tx.set(artifactsByDependencies.get(dependencyAbbrev).get(abbrev).pack(), EMPTY_BYTES);
                  tx.set(dependenciesByArtifacts.get(abbrev).get(dependencyAbbrev).pack(), EMPTY_BYTES);
                  sets.add(2);
                });
                return ReadyFuture.DONE;
              }
            });
            if (!skipJars) {
              List<Extract> extracts = extractJar(file, error);
              flowControl(db);
              db.runAsync(new Function<Transaction, Future<Void>>() {
                @Override
                public Future<Void> apply(Transaction tx) {
                  tx.set(artifactsByGroupId.get(groupId).get(artifactId).get(version).pack(), EMPTY_BYTES);
                  tx.set(artifactsByArtifactId.get(artifactId).get(groupId).get(version).pack(), EMPTY_BYTES);
                  count.increment();
                  sets.add(2);
                  return ReadyFuture.DONE;
                }
              });
              Set<String> packages = new ConcurrentSkipListSet<>();
              for (int i = 0; i < extracts.size(); i += 1000) {
                final int finalI = i;
                db.runAsync(new Function<Transaction, Future<Void>>() {
                  @Override
                  public Future<Void> apply(Transaction tx) {
                    int min = Math.min(finalI + 1000, extracts.size());
                    extracts.subList(finalI, min).parallelStream().forEach(extract -> {
                      tx.set(classesBySHA.get(extract.digest).get(extract.filename).pack(), EMPTY_BYTES);
                      if (packages.add(extract.pkg)) {
                        // Reduces the number of writes
                        tx.set(artifactsByPackage.get(extract.pkg).get(abbrev).pack(), EMPTY_BYTES);
                      }
                      tx.set(artifactsByClass.get(extract.pkg).get(extract.name).get(abbrev).pack(), EMPTY_BYTES);
                      tx.set(classesByClassname.get(extract.name).get(extract.pkg).pack(), EMPTY_BYTES);
                      sets.add(4);
                    });
                    return ReadyFuture.DONE;
                  }
                });
              }
            }
          }
        }
      } catch (IOException | XmlPullParserException | NoSuchAlgorithmException e) {
        e.printStackTrace();
        error.increment();
      }
    });
    System.out.println("Indexed " + count);
    System.out.println("Errors " + error);
    System.out.println("Sets " + sets);
  }

  private static void flowControl(Database db) {
    db.run(new Function<Transaction, Object>() {
      @Override
      public Object apply(Transaction transaction) {
        return transaction.get(EMPTY_BYTES).get();
      }
    });
  }

  private static List<Extract> extractJar(File file, LongAdder error) throws IOException, NoSuchAlgorithmException {
    JarFile jarFile = new JarFile(file);
    MessageDigest md = MessageDigest.getInstance("SHA1");
    return jarFile.stream()
            .filter(jf -> !jf.isDirectory() && jf.getName().endsWith(".class") && !jf.getName().contains("$"))
            .map(jf -> {
              Extract extract = new Extract();
              try {
                byte[] bytes = new byte[8192];
                InputStream inputStream = jarFile.getInputStream(jf);
                int len;
                while ((len = inputStream.read(bytes)) != -1) {
                  md.update(bytes, 0, len);
                }
                inputStream.close();
                extract.digest = md.digest();
                String filename = jf.getName();
                extract.filename = filename;
                int lastSlash = filename.lastIndexOf("/");
                if (lastSlash != -1) {
                  String leafFilename = filename.substring(lastSlash + 1);
                  extract.name = leafFilename.substring(0, leafFilename.length() - 6);
                  extract.pkg = filename.substring(0, lastSlash).replace('/', '.');
                } else {
                  extract.pkg = "";
                  extract.name = filename.substring(0, filename.length() - 6);
                }
              } catch (IOException e) {
                error.increment();
              }
              return extract;
            }).collect(toList());
  }

  private static String group(Model model) {
    String groupId = model.getGroupId();
    if (groupId == null) {
      Parent parent = model.getParent();
      groupId = parent.getGroupId();
    }
    return groupId;
  }

  private static String artifact(Model model) {
    String artifactId = model.getArtifactId();
    if (artifactId == null) {
      Parent parent = model.getParent();
      artifactId = parent.getArtifactId();
    }
    return artifactId;
  }

  private static String version(Model model) {
    String version = model.getVersion();
    if (version == null) {
      Parent parent = model.getParent();
      if (parent != null) {
        version = parent.getVersion();
      }
    }
    return version;
  }
}

class Extract {
  byte[] digest;
  String filename;
  String name;
  public String pkg;
}

class Artifact extends ArrayList {
  Artifact(String groupId, String artifactId, String version) {
    super(3);
    add(groupId);
    add(artifactId);
    add(version);
  }
}
