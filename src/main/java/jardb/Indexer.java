package jardb;

import com.foundationdb.Database;
import com.foundationdb.FDB;
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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Indexer {

  public static final byte[] EMPTY_BYTES = new byte[0];
  public static final int PERMITS = 100;

  @Argument
  private static boolean skipJars = false;

  @Argument(alias = "d", description = "Specify the directory to index", required = true)
  private static File directory;

  public static void main(String[] args) throws IOException, NoSuchAlgorithmException, InterruptedException {
    Args.parseOrExit(Indexer.class, args);
    if (!directory.exists()) {
      System.err.println("Directory " + directory + " doesn't exist");
      System.exit(1);
    }
    FDB fdb = FDB.selectAPIVersion(200);
    Database db = fdb.open();
    DirectoryLayer dl = DirectoryLayer.getDefault();
    DirectorySubspace jardb = dl.createOrOpen(db, asList("jardb")).get();
    Subspace metadata = jardb.get("metadata");
    DirectorySubspace artifacts = jardb.createOrOpen(db, asList("artifacts")).get();
    DirectorySubspace artifactsByGroupId = artifacts.createOrOpen(db, asList("group")).get();
    DirectorySubspace artifactsByArtifactId = artifacts.createOrOpen(db, asList("artifact")).get();
    DirectorySubspace artifactsByPackage = artifacts.createOrOpen(db, asList("package")).get();
    DirectorySubspace artifactsByClass = artifacts.createOrOpen(db, asList("class")).get();
    DirectorySubspace artifactsByDependencies = artifacts.createOrOpen(db, asList("dependencies")).get();
    DirectorySubspace dependenciesByArtifacts = artifacts.createOrOpen(db, asList("dependee")).get();
    DirectorySubspace classes = jardb.createOrOpen(db, asList("classes")).get();
    DirectorySubspace classesBySHA = classes.createOrOpen(db, asList("sha")).get();
    DirectorySubspace classesByClassname = classes.createOrOpen(db, asList("name")).get();
    LongAdder count = new LongAdder();
    LongAdder error = new LongAdder();
    LongAdder sets = new LongAdder();
    class Artifact extends ArrayList {
      Artifact(String groupId, String artifactId, String version) {
        super(3);
        add(groupId);
        add(artifactId);
        add(version);
      }
    }

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
              MessageDigest md = MessageDigest.getInstance("SHA1");
              JarFile jarFile = new JarFile(file);
              class Extract {
                byte[] digest;
                String filename;
                String name;
                public String pkg;
              }
              List<Extract> extracts = jarFile.stream()
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
              db.run(new Function<Transaction, Object>() {
                @Override
                public Object apply(Transaction transaction) {
                  return transaction.get(EMPTY_BYTES).get();
                }
              });
              db.runAsync(new Function<Transaction, Future<Void>>() {
                @Override
                public Future<Void> apply(Transaction tx) {
                  tx.set(artifactsByGroupId.get(abbrev).pack(), EMPTY_BYTES);
                  tx.set(artifactsByArtifactId.get(abbrev).pack(), EMPTY_BYTES);
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
