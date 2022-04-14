package com.netflix.spinnaker.keel.artifacts

import com.netflix.spinnaker.keel.api.ScmBridge
import com.netflix.spinnaker.keel.api.artifacts.DeliveryArtifact
import com.netflix.spinnaker.keel.api.artifacts.GitMetadata
import com.netflix.spinnaker.keel.api.artifacts.PublishedArtifact
import com.netflix.spinnaker.keel.caffeine.CacheFactory
import com.netflix.spinnaker.keel.exceptions.UnsupportedScmType
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.springframework.stereotype.Component
<<<<<<< e7716db49d60e1639d34b7e82d88196339b48c63
<<<<<<< 18a357a0e27d1429b78551a3d4cd161d807ec9ef
<<<<<<< 81a02f79ab6c85a4b20ffb80739fa11f668718b1
<<<<<<< 167ad3338605133bfc668b54b812132b3cb61e6e
import java.net.URL
=======
import org.springframework.beans.factory.annotation.Qualifier
>>>>>>> 3f7e0b13832ca29f8794caa1c2d3c3112a840a28
=======
>>>>>>> 195e071533e22e8bef9a0eb2fc259eac211bff60
=======
import org.springframework.beans.factory.annotation.Qualifier
>>>>>>> 0f5f989b898518e29173fc2f182e8ff33b632943
=======
>>>>>>> 66c49185b1c72d867c02a5bcf5de11177f8c4f32

@Component
class ArtifactVersionLinks(
  private val scmBridge: ScmBridge,
  cacheFactory: CacheFactory
) {
  private val coroutineDispatcher: CoroutineDispatcher = Dispatchers.IO
  private val cacheName = "scmInfo"
  private val cache = cacheFactory.asyncLoadingCache<Any, Map<String, String?>>(cacheName) {
    scmBridge.getScmInfo()
  }

  //Comparing 2 versions of a specific artifact, and generate a SCM comparable link based on old vs. new version
  fun generateCompareLink(version1: PublishedArtifact?, version2: PublishedArtifact?, artifact: DeliveryArtifact): String? {
    return if (version1 != null && version2 != null) {
      return if (artifact.sortingStrategy.comparator.compare(version1, version2) > 0) { //these comparators sort in dec order, so condition is flipped
        //version2 is newer than version1
        generateCompareLink(version2.gitMetadata, version1.gitMetadata)
      } else {
        //version2 is older than version1
        generateCompareLink(version1.gitMetadata, version2.gitMetadata)
      }
    } else {
      null
    }
  }

  //Generating a SCM compare link between source (new version) and target (old version) versions (the order matter!)
  private fun generateCompareLink(newerGitMetadata: GitMetadata?, olderGitMetadata: GitMetadata?): String? {
    val commitLink = newerGitMetadata?.commitInfo?.link ?: return null
    val normScmType = getNormalizedScmType(commitLink)
    val baseScmUrl = getScmBaseLink(commitLink)
    return if (normScmType != null && baseScmUrl != null && olderGitMetadata != null && !(olderGitMetadata.commitInfo?.sha.isNullOrEmpty())) {
      when {
        "stash" in normScmType -> {
          "$baseScmUrl/projects/${newerGitMetadata.project}/repos/${newerGitMetadata.repo?.name}/compare/commits?" +
            "targetBranch=${olderGitMetadata.commitInfo?.sha}&sourceBranch=${newerGitMetadata.commitInfo?.sha}"
        }
        "github" in normScmType -> {
          "$baseScmUrl/${newerGitMetadata.project}/${newerGitMetadata.repo?.name}/compare/" +
            "${olderGitMetadata.commitInfo?.sha}...${newerGitMetadata.commitInfo?.sha}"
        }
        else -> throw UnsupportedScmType(message = "Stash & GitHub are currently the only supported SCM types.")
      }
    } else null
  }

  fun getNormalizedScmType(commitLink: String): String? {
    val scmType = getScmType(commitLink)
    return scmType?.toLowerCase()
  }

  fun getScmType(commitLink: String): String? {
    val commitURL = URL(commitLink)

    val scmBaseURLs = runBlocking {
      val cachedValue = cache[cacheName].get()
      cachedValue ?: scmInfo.getScmInfo()
    }

    val base = scmBaseURLs.filter { (_,baseUrl) ->
      commitURL.host == URL(baseUrl).host
    }

    return base.keys.toList().firstOrNull()
  }

  //Calling igor to fetch all base urls by SCM type, and returning the right one based on current commit link
  fun getScmBaseLink(commitLink: String): String? {
    val normScmType = getNormalizedScmType(commitLink)
    val scmType = getScmType(commitLink)
    val scmBaseURLs = runBlocking {
      val cachedValue = withContext(coroutineDispatcher) {
        cache[cacheName].get()
      }
      cachedValue ?: scmBridge.getScmInfo()
    }

    return if (normScmType != null) {
      when {
        "stash" in normScmType ->
          scmBaseURLs["stash"]
        "github" in normScmType -> {
          val url = URL(scmBaseURLs[scmType])
          "${url.protocol}://${url.host}"
        }
        else ->
          throw UnsupportedScmType(message = "Stash and GitHub are currently the only supported SCM types.")
      }
    } else null
  }
}