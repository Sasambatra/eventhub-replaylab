document.querySelector('#login-form').addEventListener('submit', async (event) => {
  event.preventDefault();
  const button = event.target.querySelector('button');
  const error = document.querySelector('#login-error');
  button.disabled = true; error.textContent = '';
  try {
    const response = await fetch('/session', {method: 'POST', headers: {'Content-Type': 'application/json',
      'X-CSRF-Token': document.querySelector('meta[name="csrf-token"]').content},
      body: JSON.stringify({key: document.querySelector('#access-key').value})});
    if (!response.ok) throw new Error('Проверьте ключ доступа и попробуйте снова.');
    document.querySelector('#access-key').value = ''; window.location.assign('/');
  } catch (failure) { error.textContent = failure.message; button.disabled = false; }
});
