/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

function legacyCopy(text: string): boolean {
  const scratch = document.createElement('textarea')
  scratch.value = text
  scratch.setAttribute('readonly', '')
  scratch.style.position = 'fixed'
  scratch.style.opacity = '0'
  document.body.appendChild(scratch)
  scratch.select()
  let copied = false
  try {
    copied = document.execCommand('copy')
  } catch {
    copied = false
  }
  // Removed on every path: the node holds a bearer token while it is attached.
  document.body.removeChild(scratch)
  return copied
}

/**
 * Copy `text`, reporting whether it worked rather than raising a message itself.
 *
 * The caller owns the notification because this feature copies a credential, and the wording of
 * a failure ("copy it now, it will not be shown again") matters more here than elsewhere.
 * `navigator.clipboard` is unavailable on insecure origins, so the legacy path is not dead code.
 */
export async function copyToClipboard(text: string): Promise<boolean> {
  if (navigator.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(text)
      return true
    } catch {
      // Fall through: a denied permission prompt lands here.
    }
  }
  return legacyCopy(text)
}
