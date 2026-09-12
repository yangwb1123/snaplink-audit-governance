import { mount } from 'svelte'
import App from './App.svelte'
import './style.css'

const target = document.getElementById('root')
if (!target) throw new Error('Missing #root element')

export default mount(App, { target })
