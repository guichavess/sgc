class a{constructor(){this.init()}init(){this.setupForm(),this.setupPasswordToggle()}setupForm(){const e=document.getElementById("formLogin");e&&e.addEventListener("submit",t=>{var r,i;const n=(r=document.getElementById("usuario"))==null?void 0:r.value.trim(),s=(i=document.getElementById("senha"))==null?void 0:i.value;if(!n||!s){t.preventDefault(),this.showError("Preencha todos os campos");return}const o=e.querySelector('button[type="submit"]');o&&(o.disabled=!0,o.innerHTML='<span class="spinner-border spinner-border-sm"></span> Entrando...')})}setupPasswordToggle(){const e=document.getElementById("toggleSenha"),t=document.getElementById("senha");!e||!t||e.addEventListener("click",()=>{const n=t.type==="password"?"text":"password";t.type=n;const s=e.querySelector("i");s&&(s.className=n==="password"?"bi bi-eye":"bi bi-eye-slash")})}showError(e){const t=document.getElementById("alertContainer");t&&(t.innerHTML=`
            <div class="alert alert-danger alert-dismissible fade show" role="alert">
                <i class="bi bi-exclamation-circle me-2"></i>
                ${e}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `)}}document.addEventListener("DOMContentLoaded",()=>{window.loginPage=new a});
